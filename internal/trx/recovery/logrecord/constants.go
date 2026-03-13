package logrecord

const (
	checkpointBegin = iota
	checkpointEnd
	start
	end
	commit
	abort
	setInt16
	setInt32
	setStr
	setBool
	setDate
	compensationSetInt16
	compensationSetInt32
	compensationSetStr
	compensationSetBool
	compensationSetDate
)

const (
	int32Size = 4
	int16Size = 2
	boolSize  = 1
	dateSize  = 8 // 2038年問題があるため64bitで保存したい。unixtimeで秒まで保存。
)
