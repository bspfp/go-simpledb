package simpledb

import "database/sql"

type DbDoc struct {
	PK   string
	Rev  int64
	Data string

	SI map[int]string
	NI map[int]int64

	ToDelete bool `json:"-" yaml:"-"` // Put(false) or Delete(true)
}

func NewDbDoc(pk string, rev int64, data string) *DbDoc {
	return &DbDoc{
		PK:   pk,
		Rev:  rev,
		Data: data,
	}
}

func (d *DbDoc) StringIndex(index int, value string) {
	if d.SI == nil {
		d.SI = make(map[int]string)
	}
	d.SI[index] = value
}

func (d *DbDoc) Int64Index(index int, value int64) {
	if d.NI == nil {
		d.NI = make(map[int]int64)
	}
	d.NI[index] = value
}

func (d *DbDoc) Scan(rows *sql.Rows) error {
	var pk, data string
	var rev int64
	var si [MaxIndex]*string
	var ni [MaxIndex]*int64
	fields := make([]any, 3+MaxIndex*2)
	fields[0] = &pk
	fields[1] = &rev
	fields[2] = &data
	for i := 0; i < MaxIndex; i++ {
		fields[3+i*2] = &si[i]
		fields[4+i*2] = &ni[i]
	}

	if err := rows.Scan(fields...); err != nil {
		return err
	}

	d.PK = pk
	d.Rev = rev
	d.Data = data
	for i := 0; i < MaxIndex; i++ {
		if si[i] != nil {
			d.StringIndex(i, *si[i])
		} else {
			delete(d.SI, i)
		}
		if ni[i] != nil {
			d.Int64Index(i, *ni[i])
		} else {
			delete(d.NI, i)
		}
	}
	return nil
}

func (d *DbDoc) Decode() (map[string]any, error) {
	if d == nil || d.Data == "" {
		return nil, nil
	}

	return JsonDecode(d.Data)
}
