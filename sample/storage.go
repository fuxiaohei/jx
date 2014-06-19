package main

import (
	"github.com/fuxiaohei/jx"
	"fmt"
	"time"
)

type User struct {
	Id       int64 `jx:"pk-auto"`
	Name     string
	Password string
	Email    string
	Sex      string
	Age      int
	Bio      string
}

type Group struct {
	Name      string `jx:"pk"`
	Bio       string
	UserCount int
}

// todo : complete GroupUser

var (
	s *jx.Storage
	start = time.Now()
)

func init() {
	var e error
	// create new storage in "data" directory.
	// it directory is not existed, create.
	s, e = jx.NewStorage("data")
	if e != nil {
		panic(e)
	}

	// sync struct to storage.
	// it creates "sample.User" and "sample.Group" to save struct data files.
	e = s.Sync(new(User), new(Group))
	if e != nil {
		panic(e)
	}

	// try to get first one
	u := &User{Id:1} // use struct pointer &User{}
	e = s.Get(u)
	if e != nil && e != jx.Nil {
		panic(e)
	}

	// init data if the user is null
	if len(u.Name) < 1 {
		initData()
	}

	fmt.Printf("init data : %s \n", time.Since(start))
}

// write thousands data for testing
func initData() {
	for i := 0; i < 1456; i++ {
		u := &User{
			Name:randomString(6),
			Password:randomString(12),
			Email:randomString(20),
			Sex:randomString(1),
			Age:randInt(1, 99),
			Bio:randomString(100),
		}
		e := s.Insert(u)
		if e != nil {
			panic(e)
		}
	}
	for i := 0; i < 98; i++ {
		g := &Group{
			Name:randomString(4),
			Bio:randomString(50),
			UserCount:0,
		}
		e := s.Insert(g)
		if e != nil && e != jx.Conflict {
			panic(e)
		}
	}
}

func main() {

	// insert sample ----------------------
	fmt.Println("inserting ----------------- ")

	// get current chunk cursor.
	// the default chunk file limit is 1000.
	// if insert over limit, it should write to another chunk file, cursor is changed.
	currentCursor := s.Table(new(User)).Chunk.GetCurrent()
	for i := 0; i < 1111; i++ {
		u := &User{
			Name:randomString(6),
			Password:randomString(12),
			Email:randomString(20),
			Sex:randomString(1),
			Age:randInt(1, 99),
			Bio:randomString(100),
		}
		e := s.Insert(u)
		if e != nil {
			panic(e)
		}
	}
	newCursor := s.Table(new(User)).Chunk.GetCurrent()
	fmt.Printf("insert 1111 data, the chunk move to %d from %d\n", newCursor, currentCursor)

	// get sample ------------------
	fmt.Println("getting ----------------- ")

	// get the max id of auto-increment pk
	maxId := s.Table(new(User)).Pk.GetAutoIncrement()
	isFoundAll := true
	fmt.Printf("get max id is %d, so get value by random in range between 1 to max\n", maxId)
	fmt.Println("let's find 99 times")
	for i := 0; i <= 99; i++ {
		// random an id between 1 and max
		id := randInt(1, int(maxId))
		u := &User{Id:int64(id)}
		e := s.Get(u)
		if e != nil {
			if e == jx.Nil {
				fmt.Printf("got nil user by id %d\n", id)
				isFoundAll = false
				continue
			}
			panic(e)
		}
	}
	fmt.Printf("find all %v\n", isFoundAll)

	// update sample -------------------
	fmt.Println("updating ----------------")

	// we had max id of users data. so we can update one by random id.
	fmt.Println("let's update 99 times")
	for i:=0;i<=99;i++{
		id := randInt(1,int(maxId))
		u := &User{
			Id:int64(id),
			Name:randomString(6),
			Password:randomString(12),
			Email:randomString(20),
			Sex:randomString(1),
			Age:randInt(1, 99),
			Bio:randomString(100),
		}
		e :=s.Update(u)
		if e != nil{
			panic(e)
		}
	}
	fmt.Println("update all true")


	// delete sample ---------------
	fmt.Println("deleting -------------")

	// same as updating, random it to delete
	fmt.Println("let's delete 99 items")
	for i := 0; i <= 99; i++ {
		// random an id between 1 and max
		id := randInt(1, int(maxId))
		u := &User{Id:int64(id)}
		e := s.Delete(u)
		if e != nil {
			panic(e)
		}
	}
	fmt.Println("delte all true")
}
