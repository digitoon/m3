	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/generated/proto/schema"
	"github.com/m3db/m3x/time"
	infoFdWithDigest           digest.FdWithDigestWriter
	indexFdWithDigest          digest.FdWithDigestWriter
	dataFdWithDigest           digest.FdWithDigestWriter
	digestFdWithDigestContents digest.FdWithDigestContentsWriter
	checkpointFilePath         string
	digestBuf    digest.Buffer
	err          error
	bufferSize int,
		blockSize:                  blockSize,
		filePathPrefix:             filePathPrefix,
		newFileMode:                options.GetNewFileMode(),
		newDirectoryMode:           options.GetNewDirectoryMode(),
		infoBuffer:                 proto.NewBuffer(nil),
		indexBuffer:                proto.NewBuffer(nil),
		varintBuffer:               proto.NewBuffer(nil),
		infoFdWithDigest:           digest.NewFdWithDigestWriter(bufferSize),
		indexFdWithDigest:          digest.NewFdWithDigestWriter(bufferSize),
		dataFdWithDigest:           digest.NewFdWithDigestWriter(bufferSize),
		digestFdWithDigestContents: digest.NewFdWithDigestContentsWriter(bufferSize),
		digestBuf:                  digest.NewBuffer(),
		idxData:                    make([]byte, idxLen),
	shardDir := shardDirPath(w.filePathPrefix, shard)
	w.err = nil

	var infoFd, indexFd, dataFd, digestFd *os.File
	if err := openFiles(
			filepathFromTime(shardDir, blockStart, infoFileSuffix):   &infoFd,
			filepathFromTime(shardDir, blockStart, indexFileSuffix):  &indexFd,
			filepathFromTime(shardDir, blockStart, dataFileSuffix):   &dataFd,
			filepathFromTime(shardDir, blockStart, digestFileSuffix): &digestFd,
	); err != nil {
		return err
	}

	w.infoFdWithDigest.Reset(infoFd)
	w.indexFdWithDigest.Reset(indexFd)
	w.dataFdWithDigest.Reset(dataFd)
	w.digestFdWithDigestContents.Reset(digestFd)

	return nil
	written, err := w.dataFdWithDigest.WriteBytes(data)
	if w.err != nil {
		return w.err
	}

	if err := w.writeAll(key, data); err != nil {
		w.err = err
		return err
	}
	return nil
}

func (w *writer) writeAll(key string, data [][]byte) error {
	if _, err := w.indexFdWithDigest.WriteBytes(w.varintBuffer.Bytes()); err != nil {
	if _, err := w.indexFdWithDigest.WriteBytes(entryBytes); err != nil {
func (w *writer) close() error {
	if _, err := w.infoFdWithDigest.WriteBytes(w.infoBuffer.Bytes()); err != nil {
	if err := w.digestFdWithDigestContents.WriteDigests(
		w.infoFdWithDigest.Digest().Sum32(),
		w.indexFdWithDigest.Digest().Sum32(),
		w.dataFdWithDigest.Digest().Sum32(),
	); err != nil {
	if err := closeAll(
		w.infoFdWithDigest,
		w.indexFdWithDigest,
		w.dataFdWithDigest,
		w.digestFdWithDigestContents,
	); err != nil {
		return err
	}

	return nil
}

func (w *writer) Close() error {
	err := w.close()
	if w.err != nil {
		return w.err
	}
	if err != nil {
		w.err = err
		return err
	}
	// NB(xichen): only write out the checkpoint file if there are no errors
	// encountered between calling writer.Open() and writer.Close().
	if err := w.writeCheckpointFile(); err != nil {
		w.err = err
		return err
	}
	return nil
	defer fd.Close()
	if err := w.digestBuf.WriteDigestToFile(fd, w.digestFdWithDigestContents.Digest().Sum32()); err != nil {
		return err
	}