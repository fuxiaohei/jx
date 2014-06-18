package jx

import (
	"bytes"
	"math/rand"
	"os"
	"testing"
	"time"
)

var (
	s *Storage
)

type User struct {
	Id    int64 `jx:"pk-auto"`
	Name  string
	Email string
	Sex   string
	Age   int
}

func randomString(l int) string {
	var result bytes.Buffer
	var temp string
	for i := 0; i < l; {
		if string(randInt(65, 90)) != temp {
			temp = string(randInt(65, 90))
			result.WriteString(temp)
			i++
		}
	}
	return result.String()
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
	e := os.RemoveAll("_test")
	if e != nil {
		panic(e)
	}

	s, e = NewStorage("_test")
	if e != nil {
		panic(e)
	}
	if e = s.Sync(new(User)); e != nil {
		panic(e)
	}

	for i := 0; i < 99; i++ {
		u := &User{
			Name:  randomString(8),
			Email: randomString(20),
			Sex:   randomString(1),
			Age:   randInt(1, 99),
		}
		e = s.Insert(u)
		if e != nil {
			panic(e)
		}
	}

	// wait one second for pk chan
	time.Sleep(1 * time.Second)

	// flush storage, reload
	s = nil
	s, e = NewStorage("_test")
	if e != nil {
		panic(e)
	}
	if e = s.Sync(new(User)); e != nil {
		panic(e)
	}

}

func TestInsert(t *testing.T) {
	u := &User{
		Name:  "ababab",
		Email: randomString(20),
		Sex:   randomString(1),
		Age:   randInt(1, 99),
	}
	e := s.Insert(u)
	if e != nil {
		t.Error(e)
		return
	}
	if u.Id != 100 {
		t.Errorf("expect uid %d, but got %d", 100, u.Id)
		return
	}
}

func BenchmarkInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		u := &User{
			Name:  randomString(8),
			Email: randomString(20),
			Sex:   randomString(1),
			Age:   randInt(1, 99),
		}
		e := s.Insert(u)
		if e != nil {
			b.Error(e)
			return
		}
	}
}

func TestGet(t *testing.T) {
	u := &User{Id: 100}
	e := s.Get(u)
	if e != nil {
		t.Error(e)
		return
	}
	if u.Name != "ababab" {
		t.Errorf("expect name %s, but got %s", "ababab", u.Name)
	}
}

func BenchmarkGet(b *testing.B) {
	u := &User{Id: 9}
	for i := 0; i < b.N; i++ {
		e := s.Get(u)
		if e != nil {
			b.Error(e)
		}
	}
}

func TestUpdate(t *testing.T) {
	u := &User{
		Id:    100,
		Name:  "xxxxxx",
		Email: randomString(20),
		Sex:   randomString(1),
		Age:   randInt(1, 99),
	}
	e := s.Update(u)
	if e != nil {
		t.Error(e)
		return
	}
	// get updated item
	u2 := &User{Id: 100}
	e = s.Get(u2)
	if e != nil {
		t.Error(e)
		return
	}
	if u2.Name != "xxxxxx" {
		t.Errorf("expect name %s, but got %s", "xxxxxx", u2.Name)
	}
}

func BenchmarkUpdate(b *testing.B) {
	u := &User{
		Id:    9,
		Name:  "xxxxxx",
		Email: randomString(20),
		Sex:   randomString(1),
		Age:   randInt(1, 99),
	}
	for i := 0; i < b.N; i++ {
		e := s.Update(u)
		if e != nil {
			b.Error(e)
			return
		}
	}
}

func TestDelete(t *testing.T) {
	u := &User{Id: 100}
	e := s.Delete(u)
	if e != nil {
		t.Error(e)
		return
	}
	e = s.Get(u)
	if e != Nil {
		t.Errorf("expect nil, but got %s", e)
	}
}

func BenchmarkDelete(b *testing.B) {
	for i := 0; i <= b.N; i++ {
		u := &User{Id: int64(i + 1)}
		e := s.Delete(u)
		if e != nil {
			b.Error(e)
			return
		}
	}
}
