# Triggered Lambda

Custom application looks for new files in particular folders an S3 bucket and indexes the data in ElasticSearch 

## Installing Locally

### Step 1: Clone the repository
Clone this repository

```bash
$ git clone {url}
```
or download directly from GitHub.

Change into the application directory

### Step 2: Use npm
Download node and npm and use the `install` command to read the dependencies JSON file 

```bash
$ npm install
```

### Step 3: AWS Setup

1. Install AWS Commandline tools
- https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html
I reccomend using pip.
2. Create an AWS user in IAM console. Give it appropriate permissions. Copy the key and secret for this user to use in the CLI. 
3. Configure the commandline tools - https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html

- Make sure you add 
-- key/secret
-- region

### Step 4: Setup ElasticSearch
1. Use the AWS Console to create an ElasticSearch instance - https://console.aws.amazon.com/es/home
    1. Deployment Type - Development and testing
    2. Version - 6.4
    3. instance name - elastic-search-index-test
    4. type instance - t2.small.elasticsearch 
    5. number of instances - 1
    6. take the rest of the default settings
2. Note this will not configure Kibana access!
    
### Step 5: Create an S3 Bucket for the files
1. Use the AWS Console to create a bucket. Note your bucket name!!!
2. Create folder circEvents/
3. Add a sample csv file of WMS Circulation events.
4. Create folder items/
5. Add a sample txt file of item data.

### Step 6: Test application
1. Alter s3_event.json to point to your bucket and your sample txt file.

2. Use serverless to test locally

```bash
serverless invoke local --function indexItemCSV --path s3_event.json
```

3. Alter s3_circfile_event.json to point to your bucket and your sample csv file.

4. Use serverless to test locally

```bash
serverless invoke local --function indexCircEvent --path s3_circfile_event.json
```

## Installing in AWS Lambda

1. Download and setup the application, see Installing locally
2. Deploy the code using serverless

```bash
$ serverless deploy
```
