package main

import (
	"fmt"
	"github.com/fzzy/radix/redis"
	"log"
	"reflect"
	"strings"
	"time"
)

type Tagger interface {
	Init() error
	Get(tag string, prop string) (interface{}, error)
	Set(Tag string, prop string, args ...interface{}) error
}

// tag manager -----------------------------------------------------------------

type TagManager struct {
	Name string
	Tags []Tagger
}

func (t *TagManager) String() string {
	return fmt.Sprintf("TagManager{Name: %s, Tags#len: %d}", t.Name, len(t.Tags))
}

func (t *TagManager) Init() error {
	return nil
}

func (t *TagManager) Get(tag string, prop string) (interface{}, error) {
	c, err := t.getTag(tag)
	if err != nil {
		return nil, err
	}
	return concreteGetProp(c, prop)
}

func (t *TagManager) Set(tag string, prop string, args ...interface{}) error {
	c, err := t.getTag(tag)
	if err != nil {
		return err
	}
	return concreteSetProp(c, prop, args)
}

func (t *TagManager) Append(tag Tagger) {
	t.updateChildTagName(tag)
	err := concreteCallMethod(tag, "Init")
	if err != nil {
		log.Printf("could not initialize tagger %s: %s\n", t, err)
	} else {
		t.Tags = append(t.Tags, tag)
	}
}

func (t *TagManager) getTag(tag string) (Tagger, error) {
	for _, c := range t.Tags {
		if !concreteNameIs(c, tag) {
			continue
		}
		return c, nil
	}
	return nil, fmt.Errorf("Tag %s not found.", tag)
}

func (t *TagManager) updateChildTagName(tag Tagger) {
	n, _ := concreteGetProp(tag, "Name")
	concreteSetProp(tag, "Name", fmt.Sprintf("%s:%s", t.Name, n))
}

func NewTagManager(name string) *TagManager {
	return &TagManager{Name: name}
}

// vector ----------------------------------------------------------------------

type Vector struct {
	conn *Client
	Name string
	Tags []string
}

func (v *Vector) String() string {
	return fmt.Sprintf("Vector{Name: %s, Tag#len: %d", v.Name, len(v.Tags))
}

func (v *Vector) Init() error {
	return nil
}

func (v *Vector) Get(tag string, prop string) (interface{}, error) {
	return v.conn.Get(v.key(tag, prop))
}

func (v *Vector) Set(tag string, prop string, args ...interface{}) error {
	_, err := v.conn.Set(v.key(tag, prop), args)
	return err
}

func (v *Vector) Append(tag Tagger) {
	n, err := concreteGetProp(tag, "Name")
	if err != nil {
		log.Printf("Could not append %s into vector %s\n", tag, v.Name)
	} else {
		if d, ok := n.(string); ok {
			v.Tags = append(v.Tags, d)
		} else {
			log.Printf("Tag %s has improper name %s", tag, n)
		}
	}
}

func (v *Vector) key(tag string, prop string) string {
	return fmt.Sprintf("%s:%s", tag, prop)
}

func NewVector(name string, conn *Client, args ...string) *Vector {
	return &Vector{
		conn: conn,
		Name: name,
		Tags: args,
	}
}

// tag -------------------------------------------------------------------------

type Tag struct {
	conn        *Client
	psconn      *PSClient
	Name        string
	Description string
	Value       int
	Quality     int
	Timestamp   int64
}

func (t *Tag) String() string {
	return fmt.Sprintf(
		"Tag{Name: %s, Description: %s, Value: %d, Quality: %d, Timestamp: %d}",
		t.Name,
		t.Description,
		t.Value,
		t.Quality,
		t.Timestamp,
	)
}

func (t *Tag) Init() error {
	t.conn.Multi()
	t.conn.Add("set", t.key(t.Name, "name"), t.Name)
	t.conn.Add("set", t.key(t.Name, "description"), t.Description)
	t.conn.Add("set", t.key(t.Name, "value"), t.Value)
	t.conn.Add("set", t.key(t.Name, "quality"), t.Quality)
	t.conn.Add("set", t.key(t.Name, "timestamp"), t.Timestamp)
	_, err := t.conn.Exec()
	if err == nil {
		t.auto()
	}
	return err
}

func (t *Tag) Get(tag string, prop string) (interface{}, error) {
	return t.conn.Get(fmt.Sprintf("%s:%s", tag, prop))
}

func (t *Tag) Set(tag string, prop string, args ...interface{}) error {
	if prop == "Timestamp" || prop == "Name" {
		return fmt.Errorf("%s property is not user editable.", prop)
	}
	return t.update(tag, prop, args)
}

func (t *Tag) key(tag string, prop string) string {
	return fmt.Sprintf("%s:%s", tag, prop)
}

func (t *Tag) update(tag string, prop string, args ...interface{}) error {
	now := ts()

	t.conn.Multi()
	t.conn.Add("set", t.key(tag, strings.ToLower(prop)), args)
	t.conn.Add("set", t.key(tag, "timestamp"), now)
	_, err := t.conn.Exec()

	if err != nil {
		return err
	}

	return nil
}

func (t *Tag) auto() {
	go func() {
		converter := func(prop string, v *redis.Reply) (interface{}, error) {
			switch prop {
			case "Value", "Quality":
				return v.Int()
			case "Timestamp":
				return v.Int64()
			case "Name", "Description":
				return v.Str()
			}
			return nil, fmt.Errorf("Missing case for prop %s\n", prop)
		}

		wait := 500 * time.Millisecond

		errHandler := func(msg string, args ...interface{}) {
			log.Printf(msg, args...)
			time.Sleep(wait)
		}

		t.psconn.PSubscribe(fmt.Sprintf("__keyspace@0__:%s", t.key(t.Name, "*")))
		for {
			sub := t.psconn.Receive()
			if sub.Timeout() {
				errHandler("Timedout when receiving update for %s.\n", t.Name)
				continue
			}

			s := strings.Split(strings.SplitN(sub.Channel, ":", 2)[1], ":")
			k, p := strings.Join(s[:2], ":"), strings.Title(s[2])
			v, err := t.conn.Get(strings.Join(s, ":"))
			if err != nil {
				errHandler("Couldn't get value for key:\n", strings.Join(s, ":"))
				continue
			}

			cv, err := converter(p, v)
			if err != nil {
				errHandler("Error during convertion: %s\n", err)
				continue
			}

			err = concreteSetProp(t, p, cv)
			if err != nil {
				errHandler("Couldn't set property %s to value %s in %s\n", p, v.String(), k)
				continue
			}

			time.Sleep(wait)
		}
	}()
}

func NewTag(conn *Client, psconn *PSClient, name string, description string, value int, quality int) *Tag {
	t := &Tag{
		conn:        conn,
		psconn:      psconn,
		Name:        name,
		Description: description,
		Value:       value,
		Quality:     quality,
		Timestamp:   ts(),
	}

	return t
}

func ts() int64 {
	return time.Now().UTC().Unix()
}

// reflection utilities --------------------------------------------------------

func concrete(t Tagger) reflect.Value {
	return reflect.ValueOf(t).Elem()
}

func concreteNameIs(t Tagger, name string) bool {
	v, err := concreteGetProp(t, "Name")
	if err != nil {
		return false
	}
	return v == name
}

func concreteGetProp(t Tagger, prop string) (interface{}, error) {
	r := concrete(t)
	v := r.FieldByName(prop)
	if v.Kind() == reflect.Invalid {
		return nil, fmt.Errorf("prop %s do not exist in %s", v, r)
	}
	return v.Interface(), nil
}

func concreteSetProp(t Tagger, prop string, arg interface{}) error {
	r := concrete(t)
	v := r.FieldByName(prop)

	if v.Kind() == reflect.Invalid {
		return fmt.Errorf("prop %s do not exist in %s", v, r)
	}

	nv := reflect.ValueOf(arg)
	if nv.Kind() != v.Kind() {
		return fmt.Errorf(
			"prop %s if of type %s, can't receive value of type %s",
			prop,
			v.Kind(),
			nv.Kind(),
		)
	}

	switch v.Kind() {
	case reflect.Int:
		v.SetInt(nv.Int())
	case reflect.String:
		v.SetString(nv.String())
	}

	return nil
}

func concreteCallMethod(t Tagger, method string) error {
	r := reflect.ValueOf(t)
	f := r.MethodByName(method)
	if f.Kind() == reflect.Invalid {
		return fmt.Errorf("method %s do not exist in %s", f, r)
	}
	v := f.Call([]reflect.Value{})
	if len(v) >= 1 && !v[len(v)-1].IsNil() {
		return fmt.Errorf(v[len(v)-1].String())
	}
	return nil
}
