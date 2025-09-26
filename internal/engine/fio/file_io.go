// 调用os包提供的文件操作的接口
package fio

import "os"

// FileIO 标准系统文件 IO
type FileIO struct {
	fd *os.File // 系统文件描述符
}

// NewFileIOManager 初始化标准文件 IO
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		//O_CREATE是不存在则创建
		//O_RDWR是可写可执行
		//O_APPEND是以追加方式写入
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		//类似于chmod，表示权限
		//0644-others，group，user
		//6代表可写可执行，4代表可执行
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
