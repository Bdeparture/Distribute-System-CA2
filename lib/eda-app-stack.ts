import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { DynamoEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { StartingPosition } from "aws-cdk-lib/aws-lambda";

import { Construct } from "constructs";
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class EDAAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const imagesBucket = new s3.Bucket(this, "images", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      publicReadAccess: false,
    });

    // DynamoDB Table
    const imageTable = new dynamodb.Table(this, 'ImageTable', {
      partitionKey: { name: 'ImageId', type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Only for demonstration purpose. Do not use in production.
      stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES
    });


    // Integration infrastructure
    const badImageQueue = new sqs.Queue(this, "bad-orders-q", {
      retentionPeriod: cdk.Duration.minutes(15),
    });

    const imageProcessQueue = new sqs.Queue(this, "img-created-queue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
      deadLetterQueue: {
        queue: badImageQueue,
        // # of rejections by consumer (lambda function)
        maxReceiveCount: 2,    // Changed
      },
    });

    // Lambda functions

    const processImageFn = new lambdanode.NodejsFunction(
      this,
      "ProcessImageFn",
      {
        runtime: lambda.Runtime.NODEJS_18_X,
        entry: `${__dirname}/../lambdas/processImage.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
        environment: {
          REGION: 'eu-west-1',
          DLQ_URL: badImageQueue.queueUrl,
        },
      }
    );

    const confirmationMailerFn = new lambdanode.NodejsFunction(this, "confirmationMailerFunction", {
      runtime: lambda.Runtime.NODEJS_16_X,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(3),
      entry: `${__dirname}/../lambdas/confirmationMailer.ts`,
    });

    const processDeleteFn = new lambdanode.NodejsFunction(
      this,
      "ProcessDeleteFn",
      {
        runtime: lambda.Runtime.NODEJS_16_X,
        entry: `${__dirname}/../lambdas/processDelete.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 256,
        environment: {
          DYNAMODB_TABLE_NAME: imageTable.tableName,
        },
      }
    );

    const deleteMailerFn = new lambdanode.NodejsFunction(
      this,
      "DeleteMailerFn",
      {
        runtime: lambda.Runtime.NODEJS_16_X,
        entry: `${__dirname}/../lambdas/deleteMailer.ts`,
        timeout: cdk.Duration.seconds(3),
        memorySize: 1024,
      }
    );

    const rejectionMailerFn = new lambdanode.NodejsFunction(
      this,
      "RejectionMailerFunction",
      {
        runtime: lambda.Runtime.NODEJS_16_X,
        entry: `${__dirname}/../lambdas/rejectionMailer.ts`,
        timeout: cdk.Duration.seconds(3),
        memorySize: 1024,
      }
    );

    const newImageTopic = new sns.Topic(this, "NewImageTopic", {
      displayName: "New Image topic",
    });

    const handleImageTopic = new sns.Topic(this, "HandleImageTopic", {
      displayName: "Handle Image topic",
    });

    // S3 --> SNS
    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(newImageTopic)  // Changed
    );

    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_REMOVED_DELETE,
      new s3n.SnsDestination(handleImageTopic)
    );

    // Subscribers


    //SNS
    newImageTopic.addSubscription(
      new subs.SqsSubscription(imageProcessQueue)
    );

    newImageTopic.addSubscription(
      new subs.SqsSubscription(badImageQueue)
    );

    handleImageTopic.addSubscription(
      new subs.LambdaSubscription(processDeleteFn)
    );
    //direct subscriber to the topic
    newImageTopic.addSubscription(
      new subs.LambdaSubscription(confirmationMailerFn)
    );

    // DynamoDB --> Lambda
    deleteMailerFn.addEventSource(
      new DynamoEventSource(imageTable, {
        startingPosition: StartingPosition.TRIM_HORIZON,
        batchSize: 5,
        bisectBatchOnError: true,
        retryAttempts: 2,
        enabled: true,
      })
    );

    // SQS --> Lambda
    const newImageEventSource = new events.SqsEventSource(imageProcessQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(10),
    });

    processImageFn.addEventSource(newImageEventSource);
    confirmationMailerFn.addEventSource(newImageEventSource);
    rejectionMailerFn.addEventSource(
      new events.SqsEventSource(badImageQueue, {
        batchSize: 5,
        maxBatchingWindow: cdk.Duration.seconds(10),
      })
    );

    processImageFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["sqs:SendMessage"],
        resources: [badImageQueue.queueArn],
      })
    );

    confirmationMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    rejectionMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    deleteMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    processDeleteFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["dynamodb:DeleteItem"],
        resources: [imageTable.tableArn],
      })
    );

    // Dynamo DB Permissions
    processImageFn.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ["dynamodb:PutItem", "dynamodb:GetItem"],
      resources: [imageTable.tableArn],
    }));

    processImageFn.addEnvironment("DYNAMODB_TABLE_NAME", imageTable.tableName);
    imageTable.grantReadWriteData(processImageFn);

    // Permissions
    imagesBucket.grantRead(processImageFn);
    imageTable.grantReadWriteData(processImageFn);
    imageTable.grantReadWriteData(processDeleteFn);

    // Output

    new cdk.CfnOutput(this, "bucketName", {
      value: imagesBucket.bucketName,
    });
    new cdk.CfnOutput(this, "imageItemsTableName", {
      value: imageTable.tableName,
    });
    new cdk.CfnOutput(this, "handleImageTopicArn", {
      value: handleImageTopic.topicArn,
    });
  }

}