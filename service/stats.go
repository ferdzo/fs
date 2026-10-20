package service

// DiskUsage returns the total size of chunk files on disk. It backs the
// dashboard's physical-bytes and dedup-ratio figures.
func (s *ObjectService) DiskUsage() (int64, error) {
	return s.blob.DiskUsage()
}
