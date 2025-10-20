package pd

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
	regionpkg "nyxdb/internal/region"
)

type regionMetadataStore interface {
	Put(regionpkg.Region) error
	Delete(regionpkg.ID) error
	ForEach(func(regionpkg.Region) error) error
	Close() error
	LoadTSO() (uint64, error)
	SaveTSO(uint64) error
}

type boltRegionStore struct {
	db *bolt.DB
}

const (
	boltRegionFileName  = "pd.regions"
	boltRegionBucketKey = "regions"
	boltMetaBucketKey   = "meta"
)

const regionKeyPrefix = "region/"

func newBoltRegionStore(dir string) (*boltRegionStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("pd directory is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	filePath := filepath.Join(dir, boltRegionFileName)
	db, err := bolt.Open(filePath, 0o600, &bolt.Options{Timeout: 0})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(boltRegionBucketKey)); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists([]byte(boltMetaBucketKey))
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &boltRegionStore{db: db}, nil
}

func (b *boltRegionStore) Put(region regionpkg.Region) error {
	data, err := json.Marshal(region)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", regionKeyPrefix, region.ID))
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(boltRegionBucketKey))
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", boltRegionBucketKey)
		}
		return bucket.Put(key, data)
	})
}

func (b *boltRegionStore) Delete(id regionpkg.ID) error {
	key := []byte(fmt.Sprintf("%s%d", regionKeyPrefix, id))
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(boltRegionBucketKey))
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", boltRegionBucketKey)
		}
		return bucket.Delete(key)
	})
}

func (b *boltRegionStore) ForEach(fn func(regionpkg.Region) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(boltRegionBucketKey))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, v []byte) error {
			var region regionpkg.Region
			if err := json.Unmarshal(v, &region); err != nil {
				return err
			}
			return fn(region)
		})
	})
}

func (b *boltRegionStore) Close() error {
	return b.db.Close()
}

func (b *boltRegionStore) LoadTSO() (uint64, error) {
	var value uint64
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(boltMetaBucketKey))
		if bucket == nil {
			return nil
		}
		data := bucket.Get([]byte("ts"))
		if len(data) == 0 {
			return nil
		}
		value = binary.BigEndian.Uint64(data)
		return nil
	})
	return value, err
}

func (b *boltRegionStore) SaveTSO(ts uint64) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(boltMetaBucketKey))
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", boltMetaBucketKey)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], ts)
		return bucket.Put([]byte("ts"), buf[:])
	})
}
