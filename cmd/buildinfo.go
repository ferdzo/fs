package cmd

type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

func (b BuildInfo) normalized() BuildInfo {
	if b.Version == "" {
		b.Version = "dev"
	}
	if b.Commit == "" {
		b.Commit = "none"
	}
	if b.Date == "" {
		b.Date = "unknown"
	}
	return b
}
