package db

import (
	"fmt"

	"nyxdb/internal/utils"
)

// CreateSnapshot creates a gzipped tarball of the database directory excluding
// raft and cluster metadata. The caller is responsible for persisting the
// returned bytes.
func (db *DB) CreateSnapshot() ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	exclude := map[string]struct{}{
		fileLockName: {},
		"raft":       {},
		"cluster":    {},
	}
	data, err := utils.TarGzDir(db.options.DirPath, exclude)
	if err != nil {
		return nil, fmt.Errorf("snapshot database: %w", err)
	}
	return data, nil
}
