package xxl

/**
用来日志查询，显示到xxl-job-admin后台
*/

type LogHandler func(req LogParam) Return[LogResult]

// defaultLogHandler 没有日志存储时返回真实空页，不伪造内容。
func defaultLogHandler(req LogParam) Return[LogResult] {
	return Return[LogResult]{
		Code: SuccessCode,
		Msg:  "",
		Data: LogResult{
			FromLineNum: req.FromLineNum,
			ToLineNum:   req.FromLineNum - 1,
			LogContent:  "",
			IsEnd:       true,
		},
	}
}

// reqErrLogHandler 把日志查询错误转换为协议响应。
func reqErrLogHandler(err error) Return[LogResult] {
	return Return[LogResult]{
		Code: FailureCode,
		Msg:  err.Error(),
		Data: LogResult{
			FromLineNum: 0,
			ToLineNum:   0,
			LogContent:  err.Error(),
			IsEnd:       true,
		},
	}
}
