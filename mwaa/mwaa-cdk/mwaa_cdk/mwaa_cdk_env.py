from aws_cdk import core
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_mwaa as mwaa
import aws_cdk.aws_iam as iam
import aws_cdk.aws_secretsmanager as secretsmanager

class MwaaCdkStackEnv(core.Stack):

    def __init__(self, scope: core.Construct, id: str, vpc, mwaa_props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        #Get Requirements S3 object version for MWAA config below

        dags_bucket = s3.Bucket.from_bucket_name(self, "dags", f"{mwaa_props['dagss3location']}")
        dags_bucket_arn = dags_bucket.bucket_arn

        datalake_bucket = s3.Bucket.from_bucket_name(self, "data-lake", f"{mwaa_props['datalake_bucket']}")
        datalake_bucket_arn = datalake_bucket.bucket_arn


        # Create policy to create Glue database and add permissions to kick of Glue Crawler

        mwaa_glue_policy_document = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"{datalake_bucket_arn}/*",
                        f"{datalake_bucket_arn}*"
                        ]
                    )
            ]
        )

        mwaa_glue_service_role = iam.Role(
            self,
            "mwaa-glue-timestream-service-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow.amazonaws.com"),
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
                iam.ServicePrincipal("glue.amazonaws.com"),
                
            ),
            inline_policies={"CDKmwaaGluePolicyDocument": mwaa_glue_policy_document} ,
            path="/service-role/"
        )

        # We need to get the arn for this to add to a policy later

        mwaa_glue_service_role_arn = mwaa_glue_service_role.role_arn

        # We need to add the AWS Glue Managed Service role

        managed_glue_policy = iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        mwaa_glue_service_role.add_managed_policy(managed_glue_policy)

        # Create MWAA IAM Policies and Roles, copied from MWAA documentation site

        mwaa_policy_document = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=["airflow:PublishMetrics"],
                    effect=iam.Effect.ALLOW,
                    resources=[f"arn:aws:airflow:{self.region}:{self.account}:environment/{mwaa_props['mwaa_env']}"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:ListAllMyBuckets"
                    ],
                    effect=iam.Effect.DENY,
                    resources=[
                        f"{dags_bucket_arn}/*",
                        f"{dags_bucket_arn}"
                        ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:*"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"{dags_bucket_arn}/*",
                        f"{dags_bucket_arn}"
                        ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogStream",
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                        "logs:GetLogRecord",
                        "logs:GetLogGroupFields",
                        "logs:GetQueryResults"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:airflow-{mwaa_props['mwaa_env']}-*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "logs:DescribeLogGroups"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "sqs:ChangeMessageVisibility",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[f"arn:aws:sqs:{self.region}:*:airflow-celery-*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "iam:GetRole",
                        "iam:PassRole"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[f"{mwaa_glue_service_role_arn}"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "kms:Decrypt",
                        "kms:DescribeKey",
                        "kms:GenerateDataKey*",
                        "kms:Encrypt",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                    conditions={
                        "StringEquals": {
                            "kms:ViaService": [
                                f"sqs.{self.region}.amazonaws.com",
                                f"s3.{self.region}.amazonaws.com",
                            ]
                        }
                    },
                ),
            ]
        )

        mwaa_service_role = iam.Role(
            self,
            "mwaa-service-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow.amazonaws.com"),
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
                iam.ServicePrincipal("glue.amazonaws.com"),
                
            ),
            inline_policies={"CDKmwaaPolicyDocument": mwaa_policy_document},
            path="/service-role/"
        )

        # Add permissions to TimeStream to 1/access datalake, 2/access to query TS data

        mwaa_service_role.from_role_arn(
            self,
            "timestream-permissions-from-other-stack",
            f"{mwaa_props['mwaa_ts_iam_arn']}"
        )

        # Create MWAA Security Group and get networking info

        security_group = ec2.SecurityGroup(
            self,
            id = "mwaa-sg",
            vpc = vpc,
            security_group_name = "mwaa-sg"
        )

        security_group_id = security_group.security_group_id
        
        #security_group.connections.allow_from_any_ipv4(ec2.Port.all_traffic(),"MWAA")
        security_group.connections.allow_internally(ec2.Port.all_traffic(),"MWAA")

        subnets = [subnet.subnet_id for subnet in vpc.private_subnets]
        network_configuration = mwaa.CfnEnvironment.NetworkConfigurationProperty(
            security_group_ids=[security_group_id],
            subnet_ids=subnets,
        )

        # Configure specific MWAA settings - you can externalise these if you want

        logging_configuration = mwaa.CfnEnvironment.LoggingConfigurationProperty(
            task_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            worker_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            scheduler_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            webserver_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO")
            )

        # these are custom MWAA / Apache Airflow configurations
        # these currently enable AWS Secrets manager for lookups for variables
        # you can change the prefix "airflow/connections" and "airflow/variables"
        # to ones you want to use

        options = {
            'core.load_default_connections': False,
            'core.load_examples': False,
            'webserver.dag_default_view': 'tree',
            'secrets.backend' : 'airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend',
            'secrets.backend_kwargs' : '{"variables_prefix" : "airflow/variables"}',
            'webserver.dag_orientation': 'TB'
            }

        # You need to provide access to AWS Secrets that can be used by MWAA and grant access
        # to MWAA execution role. Use this if you want to define variables for your environment
        # First create the secrets and then grab the secrets ARN which you can put in here

        mwaa_secrets_policy_document = iam.Policy(self, "MWAASecrets", 
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "secretsmanager:GetSecretValue",
                                "secretsmanager:DescribeSecret"
                            ],
                            effect=iam.Effect.ALLOW,
                            resources=[f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:/{mwaa_props['mwaa_secrets']}/*"],
                        ),
                    ]
        )
        mwaa_service_role.attach_inline_policy(mwaa_secrets_policy_document)

        tags = {
            'env': f"{mwaa_props['mwaa_env']}",
            'service': 'MWAA Apache AirFlow'
        }
        
        # Create MWAA environment using all the info above

        managed_airflow = mwaa.CfnEnvironment(
            scope=self,
            id='airflow-test-environment',
            name=f"{mwaa_props['mwaa_env']}",
            airflow_configuration_options={'core.default_timezone': 'utc'},
            #airflow_version='1.10.12',
            airflow_version='2.0.2',
            dag_s3_path="dags",
            environment_class='mw1.small',
            execution_role_arn=mwaa_service_role.role_arn,
            #kms_key=key.key_id,
            logging_configuration=logging_configuration,
            max_workers=5,
            network_configuration=network_configuration,
            #plugins_s3_object_version=None,
            #plugins_s3_path=None,
            #requirements_s3_object_version=None,
            requirements_s3_path="requirements/requirements.txt",
            source_bucket_arn=dags_bucket_arn,
            webserver_access_mode='PUBLIC_ONLY',
            #weekly_maintenance_window_start=None
        )

        managed_airflow.add_override('Properties.AirflowConfigurationOptions', options)
        managed_airflow.add_override('Properties.Tags', tags)

        core.CfnOutput(
            self,
            id="MWAASecurityGroup",
            value=security_group_id,
            description="VPC ID for MWAA"
        )


    



