package gojx

import (
	"os"
	"testing"
)

var (
	s          *Storage
	insertUser *user
)

type user struct {
	Id       int    `jx:"pk"`
	UserName string `jx:"index"`
	Password string
	Email    string `jx:"index"`
}

func my_init() {
	os.RemoveAll("test")
	s, _ = NewStorage("test")
	s.Register(new(user))
	insertUser = &user{0, "username", "password", "email"}
}

func insert() {
	for i := 0; i < 888; i++ {
		s.Put(insertUser)
	}
}

func BenchmarkInsertData(b *testing.B) {
	b.StopTimer()
	my_init()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Put(insertUser)
	}
}

func BenchmarkInsertDataEach(b *testing.B) {
	b.StopTimer()
	my_init()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Put(insertUser)
		s.Put(insertUser)
		s.Put(insertUser)
		s.Put(insertUser)
	}
}

func BenchmarkInsertDataMulti(b *testing.B) {
	b.StopTimer()
	my_init()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Put(insertUser, insertUser, insertUser, insertUser)
	}
}


func BenchmarkGetPkInDelayLoad(b *testing.B) {
	b.StopTimer()
	my_init()
	insert()
	b.StartTimer()
	u := &user{Id:9}
	for i := 0; i < b.N; i++ {
		s.Get(u)
	}
}

func BenchmarkGetPkInPreLoad(b *testing.B) {
	b.StopTimer()
	my_init()
	insert()
	b.StartTimer()
	u := &user{Id:911}
	for i := 0; i < b.N; i++ {
		s.Get(u)
	}
}

