package godbf

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
)

func (dt *DbfTable) parseRowInto(idx int, target interface{}, strict bool) error {
	row := dt.GetRowAsSlice(idx) // TODO(ppacher): do this wihout the stirng conversion
	fields := dt.Fields()
	value := reflect.Indirect(reflect.ValueOf(target))
	t := value.Type()

	if len(row) != len(fields) {
		return errors.New("Invalid data")
	}

	lookupMap := make(map[string]string)

	for idx, f := range fields {
		value := row[idx]
		lowerCaseName := strings.ToLower(f.Name())

		lookupMap[lowerCaseName] = value
	}

	for i := 0; i < t.NumField(); i++ {
		name := dt.getStructFieldName(t.Field(i))
		dbfVal, ok := lookupMap[name]
		if !ok {
			if strict {
				return errors.New("dbase schema does not have field " + name)
			}

			continue
		}

		_ = dbfVal

		switch fields[i].FieldType() {
		case Logical:
			// TODO(ppacher): not sure how different implementations use the boolean
			// 1 byte value
			val := dbfVal == "j" || dbfVal == "t" || dbfVal == "1" || dbfVal == "y"
			value.Field(i).SetBool(val)

		case Numeric:
			val, err := strconv.ParseInt(dbfVal, 0, 64)
			if err != nil {
				return err
			}

			value.Field(i).SetInt(val)
		}
	}

	return nil
}

func (dt *DbfTable) getStructFieldName(f reflect.StructField) string {
	name := f.Name
	if tag, ok := f.Tag.Lookup("dbf"); ok {
		name = tag
	}
	return dt.normaliseFieldName(name)
}
