package main

import (
	"fmt"
	"localstack_test/kintest"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type KinesisTestHelper struct {
	kinesisClient kinesisiface.KinesisAPI
	streamName    string
}

var testHelper KinesisTestHelper

// var _ = BeforeSuite(func() {
// 		fmt.Println("~~BEFORE")
// 		testHelper.streamName = "localstack_stream"

// 		kinesisAwsSession, err := newAWSSession("http://localhost:4568", "us-west-2")
// 		Expect(err).ToNot(HaveOccurred())

// 		testHelper.kinesisClient = kinesis.New(kinesisAwsSession)

// 		streamInput := &kinesis.CreateStreamInput{
// 			ShardCount: aws.Int64(1),
// 			StreamName: aws.String(testHelper.streamName),
// 		}

// 		output, err := testHelper.kinesisClient.CreateStream(streamInput)
// 		Expect(err).ToNot(HaveOccurred())
// 		Expect(output).ToNot(BeNil())
// 	})
// })

// var _ = AfterSuite(func() {
// 	fmt.Println("~~AFTER")
// 	streamInput := &kinesis.DeleteStreamInput{
// 		StreamName: aws.String(testHelper.streamName),
// 	}

// 	_, err := testHelper.kinesisClient.DeleteStream(streamInput)
// 	Expect(err).To(Not(HaveOccurred()))
// })

var _ = Describe("Kinesis records", func() {
	It("is set up correctly", func() {
		fmt.Println("~~SET UP")
		testHelper.streamName = "localstack_stream"

		kinesisAwsSession, err := newAWSSession("http://localhost:4568", "us-west-2")
		Expect(err).ToNot(HaveOccurred())

		testHelper.kinesisClient = kinesis.New(kinesisAwsSession)

		streamInput := &kinesis.CreateStreamInput{
			ShardCount: aws.Int64(1),
			StreamName: aws.String(testHelper.streamName),
		}

		output, err := testHelper.kinesisClient.CreateStream(streamInput)
		Expect(err).ToNot(HaveOccurred())
		Expect(output).ToNot(BeNil())
	})

	It("can retrieve records from kinesis stream", func() {
		fmt.Println("~~CREATE EVENTS")
		kintest.Run()

		expectRecords := func(recordsOutput *kinesis.GetRecordsOutput) {
			for i := range recordsOutput.Records {
				fmt.Printf("Record %v \n", i)
			}
			// Expect(len(recordsOutput.Records)).ToNot(BeZero())
			// Expect(len(recordsOutput.Records)).To(Equal(10))
		}

		retrieveStreamRecords(expectRecords)
	})

	It("cleans up after", func() {
		fmt.Println("~~TEARDOWN")
		deleteStreamInput := &kinesis.DeleteStreamInput{
			StreamName: aws.String(testHelper.streamName),
		}

		_, err := testHelper.kinesisClient.DeleteStream(deleteStreamInput)
		Expect(err).To(Not(HaveOccurred()))
	})
})

func retrieveStreamRecords(expectRecords func(recordsOutput *kinesis.GetRecordsOutput)) {
	describeStreamOutput, err := testHelper.kinesisClient.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(testHelper.streamName),
		},
	)
	Expect(err).ToNot(HaveOccurred())

	// get shards
	for _, shard := range describeStreamOutput.StreamDescription.Shards {
		params := &kinesis.GetShardIteratorInput{
			ShardId:           shard.ShardId,
			StreamName:        aws.String(testHelper.streamName),
			ShardIteratorType: aws.String("TRIM_HORIZON"),
		}

		getShardIterator, err := testHelper.kinesisClient.GetShardIterator(params)
		Expect(err).To(Not(HaveOccurred()))

		getRecordsOutput, err := testHelper.kinesisClient.GetRecords(
			&kinesis.GetRecordsInput{
				ShardIterator: getShardIterator.ShardIterator,
			},
		)
		Expect(err).To(Not(HaveOccurred()))

		expectRecords(getRecordsOutput)
	}
}

func newAWSSession(endpoint, region string) (*session.Session, error) {
	awsCfg := new(aws.Config)

	awsCfg.Region = aws.String(region)
	awsCfg.Endpoint = aws.String(endpoint)
	awsCfg.DisableSSL = aws.Bool(true)

	return session.NewSession(awsCfg)
}
