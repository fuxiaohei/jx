#gojx


### gojx - a simple storage engine by Golang.

gojx is a storage engine use simple kv map as documentation. It provides simple api methods to operate value in storage.
It can be used as *embedded* storage.

### Getting Started

`gojx` saves real data in files. so create a `*gojx.Storage` in directory.

```go
import "github.com/fuxiaohei/gojx"

s,e := gojx.New(gojx.StorageConfig{Dir: directory, Encoder: new(gojx.GobEncoder),Size:5000,Optimize:true})
if e != nil{
    panic(e) // remember error
}
```

`StorageConfig` defines storage options.

`Dir` is storage saving directory. `Encoder` is value encoder, implements of `gojx.Encoder` interface.
`Size` means how many items of value in each data file.
`Optimize` means to replace optimized file ( clean deleted marked data ) to raw data file.

##### 1. Register

Then register a struct to storage. So far storage can save the struct value.

```go
type User struct {
	Id       int    `gojx:"pk"`
	UserName string `gojx:"index"`
	Password string
	Email    string `gojx:"index"`
}

type School struct {
	Id      int `gojx:"pk"`
	Address string
	Rank    int `gojx:"index"`
}

e := s.Sync(new(User),new(School))

```

`gojx:"pk"` means primary key for this value, only support **int** type.

`gojx:"index"` means index for this value, support basic types. If field is `index`, storage can query data by condition with value in this field.

##### 2. Put

put a new value into storage:

```go
u := new(User)
u.UserName = "abcdef"
u.Password = "12345678"
u.Email = "abcdef@xyz.com"

e := s.Put(u) // u.Id is 1, as first one. remember error.
```

**Put** only support **struct pointer**.

The pk field `u.Id` is assigned as max in storage. Pk is auto increment one by one.

If you set `u.Id` is over max value in storage, use `u.Id` as max , then increase pk for next putting.

```go
u := new(User)
u.Id = 999
//.....
e := s.Put(u)   // u.Id is 999, the next putting value without pk is 1000.
if e == gojx.CONFLICT{
    println("put existing data")  // can not put a same pk data, change value use s.Set(u)
}

u2 := new(User)
u.Id = 666
//......
e = s.Put(u)  // u.Id is < 999, so use 1000 as u.Id not 666.
```

##### 3. Get

get value by pk : 

```go
u := &User{Id:100}
e := s.Get(u)
if e == gojx.NULL{
    println("get no data")
}else{
    println(u.UserName) // if found, field is filled.
}


```

**Get** only support **struct pointer** and by **pk field**.

If value is not registered, return error.

If value is found, `u` is filled by value.

##### 4. Set

set is used to update value by pk:

```go
u := new(User)
u.Id = 1
u.UserName = "mnopq"
u.Password = "9876543"
u.Email = "xyz@abc.com"

e := s.Set(u)
if e == gojx.NULL{
    // update not-exist data
}
```

##### 5. Delete

delete value by pk:

```go
u := new(User)
u.Id = 1

e := s.Del(u)
if e == gojx.NULL{
    // delete not-exist data
}
```
