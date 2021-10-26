from aws_cdk import core
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_iam as iam



class MwaaCdkStackDeployFiles(core.Stack):

    def __init__(self, scope: core.Construct, id: str, vpc, mwaa_props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
       
        # Create MWAA S3 Bucket and upload local dags 

        dags_bucket = s3.Bucket(
            self,
            "mwaa-dags",
            bucket_name=f"{mwaa_props['dagss3location'].lower()}",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        dags = s3deploy.BucketDeployment(self, "DeployDAG",
        sources=[s3deploy.Source.asset("./dags")],
        destination_bucket=dags_bucket,
        destination_key_prefix="dags",
        prune=False,
        retain_on_delete=False
        )

        # This uploads a requirements.txt file in the requirements
        # folder. If not needed, you can comment this out

        dagreqs = s3deploy.BucketDeployment(self, "DeployRequirements",
        sources=[s3deploy.Source.asset("./requirements")],
        destination_bucket=dags_bucket,
        destination_key_prefix="requirements",
        prune=False,
        retain_on_delete=False
        )
        


    



