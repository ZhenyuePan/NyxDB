package db

import (
	"errors"
	"fmt"
	"io"
	"log"
	"nyxdb/internal/layers/engine/data"
	"nyxdb/internal/layers/engine/fio"
	"nyxdb/internal/layers/engine/index"
	"nyxdb/internal/utils"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB bitcask 存储引擎实例
type DB struct {
	options         Options
	mu              *sync.RWMutex
	fileIds         []int                     // 文件 id，只能在加载索引的时候使用，不能在其他的地方更新和使用
	activeFile      *data.DataFile            // 当前活跃数据文件，可以用于写入
	olderFiles      map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index           index.Indexer             // 内存索引
	seqNo           uint64                    // 事务序列号，全局递增
	maxCommittedTs  uint64                    // 当前已提交的最大事务时间戳
	isMerging       bool                      // 是否正在 merge
	seqNoFileExists bool                      // 存储事务序列号的文件是否存在
	isInitial       bool                      // 是否是第一次初始化此数据目录
	fileLock        *flock.Flock              // 文件锁保证多进程之间的互斥
	bytesWrite      uint                      // 累计写了多少个字节
	reclaimSize     int64                     // 表示有多少数据是无效的
	readTxnMu       sync.Mutex
	activeReadTxns  map[uint64]uint64
	nextReadTxnID   uint64
}

type logEntry struct {
	key   []byte
	value []byte
	typ   data.LogRecordType
}

// ReplicatedOp 表示来自分布式复制的操作
type ReplicatedOp struct {
	Key    []byte
	Value  []byte
	Delete bool
}

func (db *DB) diagf(format string, args ...interface{}) {
	if !db.options.EnableDiagnostics {
		return
	}
	log.Printf("[nyxdb] "+format, args...)
}

func (db *DB) applyCommittedRecord(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
	oldPos := db.index.Put(key, pos)
	if typ == data.LogRecordDeleted {
		db.reclaimSize += int64(pos.Size)
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
		return
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint  // key 的总数量
	DataFileNum     uint  // 数据文件的数量
	ReclaimableSize int64 // 可以进行 merge 回收的数据量，字节为单位
	DiskSize        int64 // 数据目录所占磁盘空间大小
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	// 判断数据目录是否存在，如果不存在的话，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 || (len(entries) == 1 && entries[0].Name() == fileLockName) {
		isInitial = true
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:        options,
		mu:             new(sync.RWMutex),
		olderFiles:     make(map[uint32]*data.DataFile),
		index:          index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:      isInitial,
		fileLock:       fileLock,
		activeReadTxns: make(map[uint64]uint64),
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件中的索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	// 重置 IO 类型为标准文件 IO
	if db.options.MMapAtStartup {
		if err := db.resetIoType(); err != nil {
			return nil, err
		}
	}

	if err := db.loadSeqNo(); err != nil {
		return nil, err
	}

	if db.seqNo > db.maxCommittedTs {
		db.maxCommittedTs = db.seqNo
	}

	db.diagf("open complete: initial=%v seqNo=%d maxCommitted=%d", db.isInitial, db.seqNo, db.maxCommittedTs)

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	defer func() {
		// 释放文件锁
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
		// 关闭索引
		if err := db.index.Close(); err != nil {
			panic("failed to close index")
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.diagf("closing database: seqNo=%d maxCommitted=%d", db.seqNo, db.maxCommittedTs)

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogEntry{
		Record: data.LogRecord{
			Key:   []byte(seqNoKey),
			Value: []byte(strconv.FormatUint(db.seqNo, 10)),
			Type:  data.LogRecordNormal,
		},
	}
	encRecord, _ := data.EncodeLogEntry(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	//	关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// Backup 备份数据库，将数据文件拷贝到新的目录中
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

// Put 写入 Key/Value 数据，key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	entry := &logEntry{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
		typ:   data.LogRecordNormal,
	}

	return db.commitInternal([]*logEntry{entry}, db.options.SyncWrites)
}

// Delete 根据 key 删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	readTxn := db.beginReadTxn()
	defer db.endReadTxn(readTxn)
	if _, err := db.getValueForReadTs(key, readTxn.ts); err == ErrKeyNotFound {
		return nil
	} else if err != nil {
		return err
	}

	entry := &logEntry{
		key: append([]byte(nil), key...),
		typ: data.LogRecordDeleted,
	}

	return db.commitInternal([]*logEntry{entry}, db.options.SyncWrites)
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	// 判断 key 的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	readTxn := db.beginReadTxn()
	defer db.endReadTxn(readTxn)

	// 从内存数据结构中取出 key 对应的索引信息
	return db.getValueForReadTs(key, readTxn.ts)
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	readTxn := db.beginReadTxn()
	defer db.endReadTxn(readTxn)
	readTs := readTxn.ts
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, 0, db.index.Size())
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		valuePos := iterator.Value()
		if valuePos == nil {
			continue
		}
		if _, err := db.getValueByPositionWithReadTs(valuePos, readTs); err == ErrKeyNotFound {
			continue
		} else if err != nil {
			continue
		}
		keyCopy := append([]byte(nil), iterator.Key()...)
		keys = append(keys, keyCopy)
	}
	return keys
}

// Fold 获取所有的数据，并执行用户指定的操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	readTxn := db.beginReadTxn()
	defer db.endReadTxn(readTxn)
	readTs := readTxn.ts
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		valuePos := iterator.Value()
		if valuePos == nil {
			continue
		}
		value, err := db.getValueByPositionWithReadTs(valuePos, readTs)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

type readTxn struct {
	id uint64
	ts uint64
}

func (db *DB) beginReadTxn() *readTxn {
	db.readTxnMu.Lock()
	ts := db.maxCommittedTs
	db.nextReadTxnID++
	id := db.nextReadTxnID
	db.activeReadTxns[id] = ts
	db.readTxnMu.Unlock()
	return &readTxn{id: id, ts: ts}
}

func (db *DB) endReadTxn(txn *readTxn) {
	if txn == nil {
		return
	}
	db.readTxnMu.Lock()
	delete(db.activeReadTxns, txn.id)
	db.readTxnMu.Unlock()
}

func (db *DB) safePoint() uint64 {
	db.readTxnMu.Lock()
	defer db.readTxnMu.Unlock()
	if len(db.activeReadTxns) == 0 {
		return db.maxCommittedTs
	}
	var minTs uint64 = ^uint64(0)
	for _, ts := range db.activeReadTxns {
		if ts < minTs {
			minTs = ts
		}
	}
	if minTs == ^uint64(0) {
		return db.maxCommittedTs
	}
	return minTs
}

// ReadTxn 对外暴露的读事务句柄
type ReadTxn struct {
	db    *DB
	inner *readTxn
	once  sync.Once
}

// BeginReadTxn 开启一个快照读事务
func (db *DB) BeginReadTxn() *ReadTxn {
	inner := db.beginReadTxn()
	db.diagf("begin read txn id=%d readTs=%d", inner.id, inner.ts)
	return &ReadTxn{db: db, inner: inner}
}

// ReadTs 返回该读事务捕获的快照时间戳
func (rt *ReadTxn) ReadTs() uint64 {
	if rt == nil || rt.inner == nil {
		return 0
	}
	return rt.inner.ts
}

// Close 结束读事务
func (rt *ReadTxn) Close() {
	if rt == nil || rt.db == nil {
		return
	}
	rt.once.Do(func() {
		rt.db.endReadTxn(rt.inner)
		rt.db.diagf("end read txn id=%d", rt.inner.id)
	})
}

// Get 在读事务快照下读取数据
func (rt *ReadTxn) Get(key []byte) ([]byte, error) {
	if rt == nil || rt.db == nil {
		return nil, ErrDatabaseIsUsing
	}
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	return rt.db.getValueForReadTs(key, rt.inner.ts)
}

func (db *DB) readLogEntry(pos *data.LogRecordPos) (*data.LogEntry, error) {
	db.mu.RLock()
	var dataFile *data.DataFile
	if db.activeFile != nil && db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	db.mu.RUnlock()

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	entry, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (db *DB) getValueByPositionWithReadTs(logRecordPos *data.LogRecordPos, readTs uint64) ([]byte, error) {
	current := logRecordPos
	visited := 0
	for current != nil {
		entry, err := db.readLogEntry(current)
		if err != nil {
			return nil, err
		}

		commitTs := entry.Meta.CommitTs
		if commitTs == 0 || commitTs <= readTs {
			if entry.Record.Type == data.LogRecordDeleted {
				return nil, ErrKeyNotFound
			}
			return entry.Record.Value, nil
		}

		if entry.Meta.PrevOffset < 0 {
			return nil, ErrKeyNotFound
		}

		nextFid := entry.Meta.PrevFid
		if nextFid == 0 {
			nextFid = current.Fid
		}

		if nextFid == current.Fid && entry.Meta.PrevOffset == current.Offset {
			// 避免意外循环
			return nil, ErrKeyNotFound
		}

		current = &data.LogRecordPos{
			Fid:    nextFid,
			Offset: entry.Meta.PrevOffset,
			Size:   0,
		}

		visited++
		if visited > 1024 {
			return nil, ErrKeyNotFound
		}
	}
	return nil, ErrKeyNotFound
}

func (db *DB) getValueForReadTs(key []byte, readTs uint64) ([]byte, error) {
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	return db.getValueByPositionWithReadTs(pos, readTs)
}

func (db *DB) commitInternal(entries []*logEntry, forceSync bool) error {
	return db.commit(entries, 0, forceSync, false)
}

func (db *DB) commit(entries []*logEntry, commitTs uint64, forceSync bool, override bool) error {
	if len(entries) == 0 {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	var ts uint64
	if override {
		if commitTs == 0 {
			return errors.New("invalid commit timestamp")
		}
		ts = commitTs
		if ts > db.seqNo {
			db.seqNo = ts
		}
	} else {
		ts = db.seqNo + 1
		db.seqNo = ts
	}

	db.diagf("commit begin ts=%d entries=%d override=%v", ts, len(entries), override)

	localHeads := make(map[string]*data.LogRecordPos, len(entries))
	writtenPositions := make([]*data.LogRecordPos, len(entries))

	for i, entry := range entries {
		if len(entry.key) == 0 {
			return ErrKeyIsEmpty
		}

		keyStr := string(entry.key)
		var prevOffset int64 = -1
		var prevFid uint32 = 0
		if pos := localHeads[keyStr]; pos != nil {
			prevOffset = pos.Offset
			prevFid = pos.Fid
		} else if pos := db.index.Get(entry.key); pos != nil {
			prevOffset = pos.Offset
			prevFid = pos.Fid
		}

		storedKey := logRecordKeyWithSeq(entry.key, ts)
		logEntry := &data.LogEntry{
			Record: data.LogRecord{
				Key:   storedKey,
				Value: entry.value,
				Type:  entry.typ,
			},
			Meta: data.LogMeta{
				CommitTs:   ts,
				PrevFid:    prevFid,
				PrevOffset: prevOffset,
			},
		}

		pos, err := db.appendLogEntry(logEntry)
		if err != nil {
			return err
		}
		localHeads[keyStr] = pos
		writtenPositions[i] = pos
	}

	finishRecord := &data.LogEntry{
		Record: data.LogRecord{
			Key:  logRecordKeyWithSeq(txnFinKey, ts),
			Type: data.LogRecordTxnFinished,
		},
		Meta: data.LogMeta{CommitTs: ts},
	}
	if _, err := db.appendLogEntry(finishRecord); err != nil {
		return err
	}

	if forceSync {
		if err := db.activeFile.Sync(); err != nil {
			return err
		}
		db.bytesWrite = 0
	}

	for idx, entry := range entries {
		key := entry.key
		pos := writtenPositions[idx]
		db.applyCommittedRecord(key, entry.typ, pos)
	}

	if ts > db.maxCommittedTs {
		db.maxCommittedTs = ts
	}
	db.diagf("commit end ts=%d override=%v", ts, override)
	return nil
}

// ApplyReplicated 应用分布式复制过来的命令
func (db *DB) ApplyReplicated(commitTs uint64, ops []ReplicatedOp) error {
	if len(ops) == 0 {
		return nil
	}
	entries := make([]*logEntry, len(ops))
	for i, op := range ops {
		entry := &logEntry{
			key: append([]byte(nil), op.Key...),
		}
		if op.Delete {
			entry.typ = data.LogRecordDeleted
		} else {
			entry.typ = data.LogRecordNormal
			entry.value = append([]byte(nil), op.Value...)
		}
		entries[i] = entry
	}
	return db.commit(entries, commitTs, db.options.SyncWrites, true)
}

// MaxCommittedTs 返回当前最大事务提交时间戳
func (db *DB) MaxCommittedTs() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.maxCommittedTs
}

// LatestCommitTs returns the latest committed version timestamp for a key.
// The boolean result indicates whether a committed version exists.
func (db *DB) LatestCommitTs(key []byte) (uint64, bool, error) {
	pos := db.index.Get(key)
	if pos == nil {
		return 0, false, nil
	}
	entry, err := db.readLogEntry(pos)
	if err != nil {
		return 0, false, err
	}
	return entry.Meta.CommitTs, true, nil
}

// GetVersion returns the value of key visible at readTs.
// The boolean result indicates whether a visible version exists.
func (db *DB) GetVersion(key []byte, readTs uint64) ([]byte, bool, error) {
	value, err := db.getValueForReadTs(key, readTs)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return value, true, nil
}

// 追加写数据到活跃文件中
func (db *DB) appendLogEntry(entry *data.LogEntry) (*data.LogRecordPos, error) {
	// 判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	// 如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	encRecord, size := data.EncodeLogEntry(entry)
	// 如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化数据文件，保证已有的数据持久到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)
	// 根据用户配置决定是否持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: uint32(size)}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 遍历目录中的所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录有可能被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	//	对文件 id 进行排序，从小到大依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每个文件id，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { // 最后一个，id是最大的，说明是当前活跃文件
			db.activeFile = dataFile
		} else { // 说明是旧的数据文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有的文件id，处理文件中的记录
	db.diagf("loadIndex: start scanning %d files", len(db.fileIds))
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果比最近未参与 merge 的文件 id 更小，则说明已经从 Hint 文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			entry, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			// 解析 key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(entry.Record.Key)
			if seqNo == nonTransactionSeqNo {
				if entry.Record.Type == data.LogRecordNormal || entry.Record.Type == data.LogRecordDeleted {
					entry.Record.Key = realKey
					db.applyCommittedRecord(realKey, entry.Record.Type, logRecordPos)
				}
			} else {
				if entry.Record.Type == data.LogRecordTxnFinished {
					db.diagf("loadIndex: apply committed txn seq=%d records=%d", seqNo, len(transactionRecords[seqNo]))
					for _, txnRecord := range transactionRecords[seqNo] {
						db.applyCommittedRecord(txnRecord.Entry.Record.Key, txnRecord.Entry.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
					if seqNo > currentSeqNo {
						currentSeqNo = seqNo
					}
				} else if entry.Record.Type == data.LogRecordNormal || entry.Record.Type == data.LogRecordDeleted {
					entry.Record.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Entry: entry,
						Pos:   logRecordPos,
					})
				}
			}

			// 递增 offset，下一次从新的位置开始读取
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo
	if currentSeqNo > db.maxCommittedTs {
		db.maxCommittedTs = currentSeqNo
	}
	db.diagf("loadIndex: completed seqNo=%d", currentSeqNo)
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true

	return os.Remove(fileName)
}

// 将数据文件的 IO 类型设置为标准文件 IO
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
