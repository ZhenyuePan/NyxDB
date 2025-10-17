package cluster

import (
	"fmt"
	"path/filepath"

	regionpkg "nyxdb/internal/region"
)

func regionBaseDir(base string, id regionpkg.ID) string {
	return filepath.Join(base, "regions", fmt.Sprintf("%d", id))
}

func regionRaftDir(base string, id regionpkg.ID) string {
	return filepath.Join(regionBaseDir(base, id), "raft")
}

func regionMetaDir(base string, id regionpkg.ID) string {
	return filepath.Join(regionBaseDir(base, id), "meta")
}
