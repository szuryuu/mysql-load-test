package error

type MyError struct {
	Inner      error
	Message    []byte
	StackTrace []byte
	Misc       map[string]any
}

func Wrap(err error, message string, stackTrace []byte, misc map[string]any) *MyError {
	return &MyError{
		Inner:      err,
		Message:    []byte(message),
		StackTrace: stackTrace,
		Misc:       misc,
	}
}

func (e *MyError) Error() string {
	return string(e.Message)
}
