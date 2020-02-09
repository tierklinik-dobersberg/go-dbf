package godbf

import (
	"testing"
	"time"
)

type testStruct struct {
	Bool   bool      `dbf:"bool"`
	Date   time.Time `dbf:"date"`
	Float  float64   `dbf:"float"`
	Number int       `dbf:"number"`
	Text   string    `dbf:"text"`
}

func TestParseToStruct(t *testing.T) {
	assertNoError := func(err error) {
		if err != nil {
			t.Errorf("expected no error but got: %s", err.Error())
			t.FailNow()
		}
	}

	tableUnderTest := New(testEncoding)

	assertNoError(tableUnderTest.AddBooleanField("bool"))
	assertNoError(tableUnderTest.AddDateField("date"))
	assertNoError(tableUnderTest.AddFloatField("float", 64/8, 2))
	assertNoError(tableUnderTest.AddNumberField("number", 64/8, 0))
	assertNoError(tableUnderTest.AddTextField("text", 10))

	recordIdx := tableUnderTest.AddNewRecord()

	assertNoError(tableUnderTest.SetFieldValueByName(recordIdx, "bool", "1"))
	assertNoError(tableUnderTest.SetFieldValueByName(recordIdx, "date", "20200209"))
	assertNoError(tableUnderTest.SetFieldValueByName(recordIdx, "float", "1.40"))
	assertNoError(tableUnderTest.SetFieldValueByName(recordIdx, "number", "100"))
	assertNoError(tableUnderTest.SetFieldValueByName(recordIdx, "text", "some text!"))

	var target testStruct
	err := tableUnderTest.parseRowInto(recordIdx, &target, true)
	assertNoError(err)

	if target.Bool != true {
		t.Errorf("invalid boolean")
	}

	if year, month, day := target.Date.Date(); year != 2020 || month != time.February || day != 9 {
		t.Errorf("invalid date")
	}

	if target.Float != 1.4 {
		t.Errorf("invalid float")
	}

	if target.Number != 100 {
		t.Errorf("invalid number")
	}

	if target.Text != "some text!" {
		t.Errorf("invalid text")
	}
}
