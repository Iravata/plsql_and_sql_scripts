```markdown
# AWS - VSI Coordinator

## Description
This project is used for accessing/starting AWS resources like EMR and SageMaker for the FCTM project from local VSI servers.

## Installation
1. The project is deployed to VSI servers using a CI/CD pipeline. The configurations can be found in `jule.yml`.

## Usage
The AWS - VSI Coordinator can invoke two AWS resources specifically: EMR for generating features and SageMaker for inference. It utilizes a shell script that in turn uses an underlying Python script to invoke these resources.

### Invoking AWS Resources
To invoke EMR or SageMaker, use the following command with the required parameters:

```bash
./run_aws_services.sh <env> <date> <resource> <scenario_id> <mode>
```

Parameters:

| Parameter    | Description                                      | Possible Values   |
| ------------ | ------------------------------------------------ | ----------------- |
| `env`        | Specifies the environment.                       | `uat`, `prod`     |
| `date`       | The date for the operation, in YYYYMMDD format.  |                   |
| `resource`   | The AWS resource to invoke.                      | `emr`, `sagemaker`|
| `scenario_id`| The scenario ID.                                 |                   |
| `mode`       | The project mode to invoke on the AWS resources. |                   |


## Credits
This project was developed by the FCTM project team. We would like to thank all the contributors who have helped shape this project.

## Contact Information
For support or further information, please contact us at [your-email@example.com].
```