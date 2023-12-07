package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/pb"
	"github.com/onflow/cadence"
	jsondc "github.com/onflow/cadence/encoding/json"
	"github.com/vmihailenco/msgpack"

	"github.com/onflow/flow-go/cmd/util/cmd/common"
	flowmodel "github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module/metrics"
	storagebadger "github.com/onflow/flow-go/storage/badger"
	"github.com/onflow/flow-go/utils/io"
	"github.com/samber/lo"

	"go.mitsakis.org/workerpool"
)

func main() {
	// either use these or just hard code and fix
	baseDir := os.Args[4]
	err := os.MkdirAll(baseDir, 0644)
	if err != nil {
		panic(err)
	}
	height, err := strconv.ParseUint(os.Args[2], 10, 64)
	if err != nil {
		panic("parsing start height")
	}

	endBlockHeight, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		panic("parsing start height")
	}

	chunkSize := 250
	maxWorker := 30

	badgerDir := os.Args[1]
	db, err := badger.Open(badger.DefaultOptions(badgerDir).WithTruncate(true))
	if err != nil {
		panic(err)
	}

	defer db.Close()

	// fmt.Println("try to loop")
	events := GetServiceEvents(db)
	fmt.Println(len(events))
	fmt.Println(lo.Keys(events))

	fmt.Scanln()
	blockRange := lo.RangeFrom(height, int(endBlockHeight)-int(height))
	blockChunks := lo.Chunk(blockRange, chunkSize)

	err = os.MkdirAll(baseDir, 0644)
	if err != nil {
		panic(err)
	}

	metrics := &metrics.NoopCollector{}
	serviceEventsService := storagebadger.NewServiceEvents(metrics, db)
	allServices := common.InitStorages(db)
	blockService := allServices.Blocks

	p, err := workerpool.NewPoolSimple(maxWorker, func(job workerpool.Job[[]uint64], workerID int) error {
		var totalRead time.Duration
		start := time.Now()
		for _, height := range job.Payload {

			readStart := time.Now()

			block, err := blockService.ByHeight(height)
			if err != nil {
				fmt.Printf("ERROR block=%d %s\n", height, err.Error())
				continue
			}

			events, err := serviceEventsService.ByBlockID(block.ID())
			if err != nil {
				fmt.Printf("ERROR block=%d %s\n", height, err.Error())
				continue
			}

			if len(events) == 0 {
				fmt.Println("No service events")
				continue
			}

			transformedEvents := []OverflowEvent{}
			for _, event := range events {
				// at .find we want events in this format but feel free to transform any way you want
				oe, err := CreateOverflowEvent(event)
				if err != nil {
					fmt.Printf("ERROR block=%d %s\n", height, err.Error())
					continue
				}
				transformedEvents = append(transformedEvents, *oe)
			}

			readTime := time.Since(readStart)
			totalRead = totalRead + readTime

			save(baseDir, transformedEvents, height)
		}
		saveTime := time.Since(start)
		startIndex := job.Payload[0]

		log.Printf("%02d - %08d/%08d indexed total readTime=%20s writeTime=%20s\n", workerID, startIndex, endBlockHeight-startIndex, totalRead, saveTime-totalRead)
		return nil
	})
	if err != nil {
		panic(err)
	}

	for _, chunk := range blockChunks {
		p.Submit(chunk)
	}
	p.StopAndWait()
}

func save(baseDir string, data interface{}, id uint64) {
	fileName := fmt.Sprintf("%s/%d.json", baseDir, id)
	if _, err := os.Stat(fileName); err != nil {
		bytes, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			panic(err)
		}

		err = io.WriteFile(fileName, bytes)
		if err != nil {
			panic(err)
		}
	}
}

type OverflowEvent struct {
	Fields           map[string]interface{} `json:"fields"`
	Id               string                 `json:"id"`
	TransactionId    string                 `json:"transactionID"`
	Name             string                 `json:"name"`
	TransactionIndex uint32                 `json:"transactionIndex"`
	EventIndex       uint32                 `json:"eventIndex"`
}

func CreateOverflowEvent(event flowmodel.Event) (*OverflowEvent, error) {
	ev, err := jsondc.Decode(event.Payload)
	if err != nil {
		return nil, err
	}

	ce, ok := ev.(cadence.Event)
	if !ok {
		return nil, fmt.Errorf("not cadence event")
	}

	var fieldNames []string

	for _, eventTypeFields := range ce.EventType.Fields {
		fieldNames = append(fieldNames, eventTypeFields.Identifier)
	}

	finalFields := map[string]interface{}{}
	for id, field := range ce.Fields {
		name := fieldNames[id]

		value := CadenceValueToInterface(field)
		if value != nil {
			finalFields[name] = value
		}
	}

	eventType := fmt.Sprint(event.Type)
	oe := &OverflowEvent{
		Id:               fmt.Sprintf("%s-%d", event.TransactionID.String(), event.EventIndex),
		Fields:           finalFields,
		Name:             eventType,
		EventIndex:       event.EventIndex,
		TransactionId:    event.TransactionID.String(),
		TransactionIndex: event.TransactionIndex,
	}
	return oe, nil
}

// CadenceValueToInterface convert a candence.Value into interface{}
func CadenceValueToInterface(field cadence.Value) interface{} {
	if field == nil {
		return nil
	}

	switch field := field.(type) {
	case cadence.Optional:
		return CadenceValueToInterface(field.Value)
	case cadence.Dictionary:
		result := map[string]interface{}{}
		for _, item := range field.Pairs {
			value := CadenceValueToInterface(item.Value)
			key := getAndUnquoteString(item.Key)

			if value != nil && key != "" {
				result[key] = value
			}
		}
		if len(result) == 0 {
			return nil
		}
		return result
	case cadence.Struct:
		result := map[string]interface{}{}
		subStructNames := field.StructType.Fields

		for j, subField := range field.Fields {
			value := CadenceValueToInterface(subField)
			key := subStructNames[j].Identifier
			if value != nil {
				result[key] = value
			}
		}
		if len(result) == 0 {
			return nil
		}
		return result
	case cadence.Array:
		var result []interface{}
		for _, item := range field.Values {
			value := CadenceValueToInterface(item)
			if value != nil {
				result = append(result, value)
			}
		}
		if len(result) == 0 {
			return nil
		}
		return result

	case cadence.Int:
		return field.Int()
	case cadence.Address:
		return ensureStartsWith0x(field.String())
	case cadence.TypeValue:
		return field.StaticType
	case cadence.String:
		value := getAndUnquoteString(field)
		if value == "" {
			return nil
		}
		return value

	case cadence.UFix64:
		float, _ := strconv.ParseFloat(field.String(), 64)
		return float
	case cadence.Fix64:
		float, _ := strconv.ParseFloat(field.String(), 64)
		return float

	default:
		goValue := field.ToGoValue()
		if goValue != nil {
			return goValue
		}
		return ""
	}
}

func getAndUnquoteString(value cadence.Value) string {
	val := fmt.Sprint(value.ToGoValue())
	result, err := strconv.Unquote(val)
	if err != nil {
		result = val
		if strings.Contains(result, "\\u") || strings.Contains(result, "\\U") {
			return value.ToGoValue().(string)
		}
	}

	return result
}

func ensureStartsWith0x(in string) string {
	if strings.HasPrefix(in, "0x") {
		return in
	}
	return fmt.Sprintf("0x%s", in)
}

func GetServiceEvents(db *badger.DB) map[string][]OverflowEvent {
	eventStream := db.NewStream()
	eventStream.NumGo = 32                       // Set number of goroutines to use for iteration.
	eventStream.Prefix = []byte{0x6A}            // tx        //events
	eventStream.LogPrefix = "Find.ServiceEvents" // For identifying stream logs. Outputs to Logger.

	events := map[string][]OverflowEvent{}
	eventStream.Send = func(list *pb.KVList) error {
		for _, kv := range list.GetKv() {

			k := kv.GetKey()
			blockID := hex.EncodeToString(k[1:33])
			v := kv.GetValue()
			var event flowmodel.Event
			err := msgpack.Unmarshal(v, &event)
			if err != nil {
				return fmt.Errorf("could not decode the event: %w", err)
			}

			// at .find we want events in this format but feel free to transform any way you want
			oe, err := CreateOverflowEvent(event)
			if err != nil {
				fmt.Printf("ERROR block=%s %s\n", blockID, err.Error())
				continue
			}

			existingEvents, ok := events[blockID]
			if !ok {
				existingEvents = []OverflowEvent{}
			}
			existingEvents = append(existingEvents, *oe)
			events[blockID] = existingEvents
		}

		if err := eventStream.Orchestrate(context.Background()); err != nil {
			fmt.Println(err)
		}
		return nil
	}
	return events
}
