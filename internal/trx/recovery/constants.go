package recovery

const (
	checkpoint = iota
	start
	commit
	rollback
	setInt16
	setInt32
	setStr
	setBool
	setDate
)

const (
	int32Size         = 4
	int16Size         = 2
	boolSize          = 1
	dateSize          = 8 // 2038年問題があるため64bitで保存したい。unixtimeで秒まで保存。
	maxStrBytesLength = 1 // US ASCII のみを使用するので1文字1バイト想定
)
