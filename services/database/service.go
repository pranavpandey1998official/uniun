package database

import (
	"fmt"
	"sync"
	"uniun/pkg/domain"
	"uniun/pkg/protobuf"
	service_interfaces "uniun/pkg/service_interfaces"

	badger "github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	once             sync.Once
	singletonService *databaseService
)

// databaseService is the private concrete implementation.
type databaseService struct {
	db     *badger.DB
	dbPath string
}

// GetInstance is the singleton factory.
func GetService() service_interfaces.DatabaseService {
	once.Do(func() {
		singletonService = &databaseService{
			dbPath: "./blockstorage",
		}
	})
	return singletonService
}

// Start opens the BadgerDB database file. It must be called once at application startup.
func (s *databaseService) Start() error {
	var err error
	s.db, err = badger.Open(badger.DefaultOptions(s.dbPath))
	if err != nil {
		return fmt.Errorf("failed to open badger database: %w", err)
	}
	fmt.Println("[DatabaseService] Started and connected to DB.")
	return nil
}

// Stop closes the database connection. It should be called once on graceful shutdown.
func (s *databaseService) Stop() {
	if s.db != nil {
		s.db.Close()
		fmt.Println("[DatabaseService] Stopped and DB connection closed.")
	}
}

// StoreBlock now serializes the block using Protobuf.
func (s *databaseService) StoreBlock(block *domain.Block) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// 1. Map our internal domain.Block to the generated protobuf.Block.
		protoBlock := &protobuf.Block{
			Id:        block.ID,
			Data:      block.Data,
			Timestamp: timestamppb.New(block.Timestamp), // Convert time.Time to protobuf.Timestamp
		}

		// 2. Serialize the protobuf object into bytes.
		blockBytes, err := proto.Marshal(protoBlock)
		if err != nil {
			return fmt.Errorf("failed to marshal block to protobuf: %w", err)
		}

		// 3. Set the key and value in the database.
		err = txn.Set([]byte(block.ID), blockBytes)

		//Debugger
		// err := txn.Set([]byte("block_1760277367933788000"), []byte("hi"))
		return err
	})
}

// FetchBlock now deserializes the block using Protobuf.
func (s *databaseService) FetchBlock(id string) (*domain.Block, error) {
	var blockData []byte

	err := s.db.View(func(txn *badger.Txn) error {

		//Debugger
		// item, err := txn.Get([]byte("block_1760277367933788000"))
		item, err := txn.Get([]byte(id))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return service_interfaces.ErrBlockNotFound
			}
			return err
		}

		// Debugger
		// k := append([]byte{}, item.Key()...)
		// v, err := item.ValueCopy(nil)
		// if err != nil {
		// 	return err
		// }

		// log.Printf("DEBUG: [DatabaseService] Key=%s, Value=%s", k, v)

		// The item.Value method retrieves the byte slice.
		// We copy it to our blockData variable to use outside the transaction.
		return item.Value(func(val []byte) error {
			blockData = append([]byte{}, val...)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	// 1. Deserialize the bytes from the DB into a protobuf.Block object.
	protoBlock := &protobuf.Block{}
	if err := proto.Unmarshal(blockData, protoBlock); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block from protobuf: %w", err)
	}

	// 2. Map the generated protobuf.Block back to our internal domain.Block.
	domainBlock := &domain.Block{
		ID:        protoBlock.Id,
		Data:      protoBlock.Data,
		Timestamp: protoBlock.Timestamp.AsTime(), // Convert protobuf.Timestamp back to time.Time
	}

	return domainBlock, nil
}

// Search by chain id and latest of chain id
// Watch a thought
