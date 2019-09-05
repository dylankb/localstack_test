package main

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	kc "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/utils"
	wk "github.com/vmware/vmware-go-kcl/clientlibrary/worker"
)

func main() {
	streamName := "localstack_stream"
	regionName := "us-west-2"
	workerID := "1"
	kclConfig := cfg.NewKinesisClientLibConfig("appName", streamName, regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(1).
		WithMaxLeasesForWorker(1).
		WithKinesisEndpoint("http://localhost:4568")

	worker := wk.NewWorker(recordProcessorFactory(), kclConfig, nil)
	worker.Start()

	// Put some data into stream.
	for i := 0; i < 100; i++ {
		// Use random string as partition key to ensure even distribution across shards
		err := worker.Publish(streamName, utils.RandStringBytesMaskImpr(10), []byte("hello world"))
		if err != nil {
			log.Printf("Errorin Publish. %+v", err)
		}
	}

	// wait a few seconds before shutdown processing
	time.Sleep(10 * time.Second)
	worker.Shutdown()
}

// Record processor factory is used to create RecordProcessor
func recordProcessorFactory() kc.IRecordProcessorFactory {
	return &dumpRecordProcessorFactory{}
}

// simple record processor and dump everything
type dumpRecordProcessorFactory struct {
}

func (d *dumpRecordProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return &dumpRecordProcessor{}
}

// Create a dump record processor for printing out all data from record.
type dumpRecordProcessor struct {
}

func (dd *dumpRecordProcessor) Initialize(input *kc.InitializationInput) {
	log.Printf("Processing SharId: %v at checkpoint: %v", input.ShardId, aws.StringValue(input.ExtendedSequenceNumber.SequenceNumber))
}

func (dd *dumpRecordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	log.Print("Processing Records...")

	// don't process empty record
	if len(input.Records) == 0 {
		return
	}

	for _, v := range input.Records {
		log.Printf("Record = %s", v.Data)
	}

	// checkpoint it after processing this batch
	lastRecordSequenceNubmer := input.Records[len(input.Records)-1].SequenceNumber
	log.Printf("Checkpoint progress at: %v,  MillisBehindLatest = %v", lastRecordSequenceNubmer, input.MillisBehindLatest)
	input.Checkpointer.Checkpoint(lastRecordSequenceNubmer)
}

func (dd *dumpRecordProcessor) Shutdown(input *kc.ShutdownInput) {
	log.Printf("Shutdown Reason: %v", aws.StringValue(kc.ShutdownReasonMessage(input.ShutdownReason)))

	// When the value of {@link ShutdownInput#getShutdownReason()} is
	// {@link ShutdownReason#TERMINATE} it is required that you
	// checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
	if input.ShutdownReason == kc.TERMINATE {
		input.Checkpointer.Checkpoint(nil)
	}
}
