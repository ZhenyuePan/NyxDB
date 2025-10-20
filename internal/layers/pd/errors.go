package pd

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsRegionExistsError reports whether err represents a region already existing.
func IsRegionExistsError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrRegionExists) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.AlreadyExists
	}
	return false
}

// IsRegionNotFoundError reports whether err indicates missing region metadata.
func IsRegionNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrRegionNotFound) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.NotFound
	}
	return false
}
