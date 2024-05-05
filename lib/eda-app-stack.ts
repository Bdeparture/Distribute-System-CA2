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
import { Duration, RemovalPolicy } from "aws-cdk-lib";

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

    // Integration infrastructure
    const badImageQueue = new sqs.Queue(this, "bad-orders-q", {
      retentionPeriod: Duration.minutes(30),
    });

    const imageProcessQueue = new sqs.Queue(this, "img-created-queue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
      deadLetterQueue: {
        queue: badImageQueue,
        // # of rejections by consumer (lambda function)
        maxReceiveCount: 1,    // Changed
      },
    });

    const newImageTopic = new sns.Topic(this, "NewImageTopic", {
      displayName: "New Image topic",
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

    // S3 --> SNS
    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(newImageTopic)  // Changed
    );

    //SNS
    newImageTopic.addSubscription(
      new subs.SqsSubscription(imageProcessQueue)
    );

    newImageTopic.addSubscription(
      new subs.SqsSubscription(badImageQueue)
    );

    //direct subscriber to the topic
    newImageTopic.addSubscription(
      new subs.LambdaSubscription(confirmationMailerFn)
    );

    // DynamoDB Table
    const imageTable = new dynamodb.Table(this, 'ImageTable', {
      partitionKey: { name: 'ImageId', type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Only for demonstration purpose. Do not use in production.
    });


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

    // Output

    new cdk.CfnOutput(this, "bucketName", {
      value: imagesBucket.bucketName,
    });
    new cdk.CfnOutput(this, "imageItemsTableName", {
      value: imageTable.tableName,
    });
  }

}