package gojx

import (
	"crypto/rand"
	"os"
	"testing"
)

var (
	s          *Storage
	insertUser *user = &user{UserName: "user-name", Password: "123456", Email: "email-address"}
)

type user struct {
	Id       int    `jx:"pk"`
	UserName string `jx:"index"`
	Password string
	Email    string `jx:"index"`
}

func randomString(n int) string {
	const alphanum = "abc"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

func s_init(ins bool) {
	os.RemoveAll("test")
	initStorage()
	if ins {
		insert()
	}
}

func insert() {
	for i := 0; i < 999; i++ {
		u := new(user)
		u.UserName = randomString(3)
		u.Password = randomString(12)
		u.Email = randomString(12)
		s.Put(u)
	}
}

func initStorage() {
	s = nil
	s, _ = NewStorage("test")
	s.Register(new(user))
}

func BenchmarkInsertData(b *testing.B) {
	b.StopTimer()
	s_init(false)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Put(insertUser)
	}
}

func BenchmarkGetPkInDelayLoad(b *testing.B) {
	b.StopTimer()
	s_init(true)
	initStorage()
	b.StartTimer()
	u := &user{Id: 9}
	for i := 0; i < b.N; i++ {
		s.Get(u)
	}
}

func BenchmarkGetPkInPreLoad(b *testing.B) {
	b.StopTimer()
	s_init(true)
	initStorage()
	b.StartTimer()
	u := &user{Id: 911}
	for i := 0; i < b.N; i++ {
		s.Get(u)
	}
}

func BenchmarkGetByIndex(b *testing.B) {
	b.StopTimer()
	s_init(true)
	initStorage()
	b.StartTimer()
	u := &user{UserName: "abc"}
	for i := 0; i < b.N; i++ {
		s.GetBy(u, "UserName", true)
	}
}

func BenchmarkUpdate(b *testing.B) {
	b.StopTimer()
	s_init(true)
	initStorage()
	u := new(user)
	u.Id = 911
	u.UserName = randomString(3)
	u.Password = randomString(12)
	u.Email = randomString(12)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Update(u)
	}
}
