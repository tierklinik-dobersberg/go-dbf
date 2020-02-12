package godbf

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// RawData is treated differently by DecodeRow. In case it is used as
// a field type the raw byte slice will be copied instead of performing
// string encoding operations. Note that the returned data also have
// space padding applied
type RawData []byte

var rawDataType = reflect.TypeOf(RawData(nil))

// DecodeRow decodes the row at idx into the target
func (dt *DbfTable) DecodeRow(idx int, target interface{}, strict bool) error {
	row := dt.GetRawRowSlice(idx)

	fields := dt.Fields()
	value := reflect.Indirect(reflect.ValueOf(target))
	t := value.Type()

	if len(row) != len(fields) {
		return fmt.Errorf("Invalid data")
	}

	if reflect.ValueOf(target).Kind() != reflect.Ptr {
		return fmt.Errorf("result must be a pointer")
	}

	elem := reflect.ValueOf(target).Elem()
	if !elem.CanAddr() {
		return fmt.Errorf("result must be addressable")
	}

	if elem.Kind() != reflect.Struct {
		return fmt.Errorf("result must be pointer to a struct")
	}

	lookupMap := make(map[string][]byte)
	fieldMap := make(map[string]FieldDescriptor)

	for idx, f := range fields {
		value := row[idx]
		lowerCaseName := strings.ToLower(f.Name())

		lookupMap[lowerCaseName] = value
		fieldMap[lowerCaseName] = f
	}

	for i := 0; i < t.NumField(); i++ {
		name := dt.getStructFieldName(t.Field(i))
		if name == "-" {
			continue
		}

		descr, _ := fieldMap[name]
		dbfVal, ok := lookupMap[name]
		if !ok {
			if strict {
				return errors.New("dbase schema does not have field " + name)
			}

			continue
		}

		fieldValue := value.Field(i)
		if err := dt.decode(name, dbfVal, fieldValue, descr); err != nil {
			return fmt.Errorf("row index %d: %w", i, err)
		}
	}

	return nil
}

func (dt *DbfTable) decode(name string, input []byte, outVal reflect.Value, descr FieldDescriptor) error {
	var err error
	outputKind := getKind(outVal)
	switch outputKind {
	case reflect.Bool:
		err = dt.decodeBool(name, input, outVal, descr)
	case reflect.String:
		err = dt.decodeString(name, input, outVal, descr)
	case reflect.Float32:
		err = dt.decodeFloat(name, input, outVal, descr)
	case reflect.Int:
		err = dt.decodeInt(name, input, outVal, descr)
	case reflect.Uint:
		err = dt.decodeUint(name, input, outVal, descr)
	case reflect.Ptr:
		err = dt.decodePtr(name, input, outVal, descr)

	default:
		if outVal.Type().PkgPath() == "time" && outVal.Type().Name() == "Time" {
			err = dt.decodeDate(name, input, outVal, descr)
		} else if outVal.Type() == rawDataType {
			outVal.Set(reflect.ValueOf(input))
		} else {
			err = fmt.Errorf("'%s' has unsupported data type '%s'", name, outputKind)
		}
	}

	return err
}

func (dt *DbfTable) decodeBool(name string, input []byte, outVal reflect.Value, descr FieldDescriptor) error {
	val := dt.decodePaddedString(input)

	value := val == "y" || val == "j" || val == "1" || val == "t"
	if descr.FieldType() != Logical {
		return unconvertibleTypeError(name, reflect.Bool.String(), descr)
	}

	outVal.SetBool(value)

	return nil
}

func (dt *DbfTable) decodeString(name string, input []byte, outVal reflect.Value, descr FieldDescriptor) error {
	val := dt.decodePaddedString(input)
	outVal.SetString(val)

	return nil
}

func (dt *DbfTable) decodeFloat(name string, input []byte, outVal reflect.Value, descr FieldDescriptor) error {
	val := dt.decodePaddedString(input)

	if descr.FieldType() != Float {
		return unconvertibleTypeError(name, reflect.Float32.String(), descr)
	}

	if val == "" {
		return nil
	}

	f, err := strconv.ParseFloat(val, outVal.Type().Bits())
	if err != nil {
		return fmt.Errorf("'%s': cannot parse '%s' as float: %w", name, val, err)
	}

	outVal.SetFloat(f)
	return nil
}

func (dt *DbfTable) decodeInt(name string, input []byte, outVal reflect.Value, descr FieldDescriptor) error {
	val := dt.decodePaddedString(input)

	if descr.FieldType() != Numeric {
		return unconvertibleTypeError(name, reflect.Int.String(), descr)
	}

	if val == "" {
		return nil
	}

	i, err := strconv.ParseInt(val, 0, outVal.Type().Bits())
	if err != nil {
		return fmt.Errorf("'%s': cannot parse '%s' as int: %w", name, val, err)
	}

	outVal.SetInt(i)

	return nil
}

func (dt *DbfTable) decodeUint(name string, input []byte, outVal reflect.Value, descr FieldDescriptor) error {
	val := dt.decodePaddedString(input)

	if descr.FieldType() != Numeric {
		return unconvertibleTypeError(name, reflect.Uint.String(), descr)
	}

	if val == "" {
		return nil
	}

	i, err := strconv.ParseUint(val, 0, outVal.Type().Bits())
	if err != nil {
		return fmt.Errorf("cannot parse '%s' as uint: %w", val, err)
	}

	outVal.SetUint(i)

	return nil
}

func (dt *DbfTable) decodePtr(name string, input []byte, outVal reflect.Value, descr FieldDescriptor) error {
	if len(input) == 0 {
		if !outVal.IsNil() && outVal.CanSet() {
			nilValue := reflect.New(outVal.Type()).Elem()
			outVal.Set(nilValue)
		}

		return nil
	}

	valType := outVal.Type()
	valElemType := valType.Elem()
	if outVal.CanSet() {
		realVal := outVal
		if realVal.IsNil() {
			realVal = reflect.New(valElemType)
		}

		if err := dt.decode(name, input, reflect.Indirect(realVal), descr); err != nil {
			return err
		}

		outVal.Set(realVal)
	} else {
		if err := dt.decode(name, input, reflect.Indirect(outVal), descr); err != nil {
			return err
		}
	}

	return nil
}

func (dt *DbfTable) decodeDate(name string, input []byte, outVal reflect.Value, descr FieldDescriptor) error {
	val := dt.decodePaddedString(input)

	if descr.FieldType() != Date {
		return unconvertibleTypeError(name, "time.Time", descr)
	}

	if val == "" {
		return nil
	}

	if len(val) != 8 {
		return fmt.Errorf("'%s' is not a valid DBase date", val)
	}

	year := val[0:4]
	month := val[4:6]
	day := val[6:8]

	y, err := strconv.ParseInt(year, 10, 32)
	if err != nil {
		return fmt.Errorf("cannot parse '%s' as year: %w", year, err)
	}

	m, err := strconv.ParseInt(month, 10, 32)
	if err != nil {
		return fmt.Errorf("cannot parse '%s' as month: %w", month, err)
	}

	d, err := strconv.ParseInt(day, 10, 32)
	if err != nil {
		return fmt.Errorf("cannot parse '%s' as d: %w", day, err)
	}

	t := time.Date(int(y), time.Month(m), int(d), 0, 0, 0, 0, time.UTC)

	outVal.Set(reflect.ValueOf(t))
	return nil
}

func (dt *DbfTable) getStructFieldName(f reflect.StructField) string {
	name := f.Name
	if tag, ok := f.Tag.Lookup("dbf"); ok {
		name = tag
	}
	return dt.normaliseFieldName(name)
}

func getKind(val reflect.Value) reflect.Kind {
	kind := val.Kind()

	switch {
	case kind >= reflect.Int && kind <= reflect.Int64:
		return reflect.Int
	case kind >= reflect.Uint && kind <= reflect.Uint64:
		return reflect.Uint
	case kind >= reflect.Float32 && kind <= reflect.Float64:
		return reflect.Float32
	default:
		return kind
	}
}

func unconvertibleTypeError(name string, rType string, descr FieldDescriptor) error {
	return fmt.Errorf(
		"'%s' expected type '%s', got unconvertible type '%s'",
		name, rType, descr.FieldType())
}
