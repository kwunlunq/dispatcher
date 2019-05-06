package glob

import (
	"reflect"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
)

// SetIfNull Set field of obj to `value` if it was null
func SetIfNull(obj interface{}, field string, value interface{}) {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		tracer.Error(ProjName, " Err setting field: not a pointer!")
		return
	}
	objField := reflect.ValueOf(obj).Elem().FieldByName(field)
	if !objField.IsValid() {
		tracer.Warn(ProjName, "Not valid field")
		return
	}
	if !IsZeroValue(objField.Interface()) {
		tracer.Tracef(ProjName, "%v is not zero value: %v", field, objField)
		return
	}
	switch value.(type) {
	case int:
		objField.SetInt(int64(value.(int)))
	case string:
		objField.SetString(value.(string))
	case bool:
		objField.SetBool(value.(bool))
	default:
		tracer.Errorf(ProjName, " Err setting field: unsupported type: %v!", reflect.TypeOf(value))
	}
}

func IsZeroValue(val interface{}) bool {
	return reflect.DeepEqual(val, reflect.Zero(reflect.TypeOf(val)).Interface())
}
