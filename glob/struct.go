package glob

import (
	"log"
	"reflect"
)

// SetIfZero Set field of obj to `value` if it was null
func SetIfZero(obj interface{}, field string, value interface{}) {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		log.Println(" Err setting field: not a pointer!")
		return
	}
	objField := reflect.ValueOf(obj).Elem().FieldByName(field)
	if !objField.IsValid() {
		log.Println("Not valid field")
		return
	}
	if !IsZeroValue(objField.Interface()) {
		//log.Printf("%v is not zero value: %v\n", field, objField)
		return
	}
	switch value.(type) {
	case int:
		objField.SetInt(int64(value.(int)))
	case int64:
		objField.SetInt(value.(int64))
	case string:
		objField.SetString(value.(string))
	case bool:
		objField.SetBool(value.(bool))
	default:
		log.Printf(" Err setting field: unsupported type: %v\n!", reflect.TypeOf(value))
	}
}

func IsZeroValue(val interface{}) bool {
	return reflect.DeepEqual(val, reflect.Zero(reflect.TypeOf(val)).Interface())
}
