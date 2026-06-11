//go:generate errgen -p common.go

package errs

const (
	InternalCode Code = CommonCodeInit + iota + 1
	RedisErrCode
	ExecSqlFailedCode
	ParamValidateFailedCode
	RecordNotFoundErrCode
	DuplicateDataErrCode
	MongoErrCode
	KafkaErrCode
	EtcdErrCode
	DtmErrCode
	PulsarErrCode
)

var (
	Internal            = InternalCode.Error("")
	RedisErr            = RedisErrCode.Error("")
	ExecSqlFailed       = ExecSqlFailedCode.Error("")
	ParamValidateFailed = ParamValidateFailedCode.Error("")
	RecordNotFoundErr   = RecordNotFoundErrCode.Error("")
	DuplicateDataErr    = DuplicateDataErrCode.Error("")
	MongoErr            = MongoErrCode.Error("")
	KafkaErr            = KafkaErrCode.Error("")
	EtcdErr             = EtcdErrCode.Error("")
	DtmErr              = DtmErrCode.Error("")
	PulsarErr           = PulsarErrCode.Error("")
)
