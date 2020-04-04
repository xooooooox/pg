### A simple curd case on postgres

```go
func TestTable(t *testing.T) {
	// curd
	// select single condition
	user := User{}
	Table(&user).Print().WhereEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereNotEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereMoreThan(UserId, 100).Get(&user)
	Table(&user).Print().WhereMoreThanEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereLessThan(UserId, 100).Get(&user)
	Table(&user).Print().WhereLessThanEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereNotIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereBetween(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereOrEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrNotEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrMoreThan(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrMoreThanEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrLessThan(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrLessThanEqual(UserId, 100).Get(&user)
	Table(&user).Print().WhereOrIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereOrNotIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().WhereOrBetween(UserId, 100, 101).Get(&user)

	Table(&user).Print().WhereBetween(UserId, 100, 101).
		WhereAppend(`AND "status" = $1`,0).
		WhereAppend(`OR "time" NOT IN ( $1, $2, $3)`,-1,-2,-3).
		Get(&user)
	Table(&user).Print().Where(`"id = $1"`, 100).
		WhereAppend(`AND "status" = $1`,0).
		WhereAppend(`OR "time" NOT IN ( $1, $2, $3)`,-1,-2,-3).
		Get(&user)

	// specifying column names
	Table(&user).Print().Cols(UserId, UserName).WhereNotIn(UserId, 100, 101).Get(&user)
	Table(&user).Print().Cols(UserAvatar, UserTime).WhereOrBetween(UserId, 100, 101).Get(&user)

	// select more conditions
	Table(&user).
		Print().
		Cols(UserId, UserName).
		WhereEqual(UserId, 102).
		WhereNotIn(UserId, 100, 101).
		Group(UserId).
		Asc(UserTime).
		Get(&user)
	Table(&user).
		Print().
		WhereIn(UserId, 100, 101).
		WhereOrBracketsLeft().
		WhereEqual(UserId, 60).
		WhereOrEqual(UserId, 70).
		WhereBracketsRight().
		Get(&user)

	// winning Joint Search
	Table(&user).
		Print().
		Alias("u").
		Cols("u.id", "u.name", `sum("status") as "status"`).
		LeftJoin(&user, "v", "u.id", "v.parent").
		LeftJoin(&user, "w", "u.id", "w.parent").
		WhereBetween("u.id", 10, 100).
		WhereBracketsLeft().
		WhereMoreThanEqual("v.id", 0).
		WhereBracketsLeft().
		WhereLessThanEqual("v.id", 10000).
		WhereOrIn("v.status", 0, 1, 2, 3, 4, 5).
		WhereIn("v.status", 5, 6, 7, 8, 9).
		WhereBracketsRight().
		WhereBracketsLeft().
		WhereBetween("v.status", 0, 100).
		WhereOrMoreThanEqual("w.status", 0).
		WhereBracketsRight().
		WhereBracketsRight().
		Group("u.name").
		Desc("u.id").
		Offset(10).
		//Page(2).
		Get(&user)
	fmt.Println(user)

	// insert one row, returns the self-incrementing id of the inserted data
	tu := Table(&user).Print()
	tu.Add(&User{
		Nick: "123468900987654321",
	})
	id := tu.Id()
	fmt.Println(id)

	tu.WhereEqual("id", id).Mod(UserAvatar, "mod-avatar").Ups()
	ups := tu.Rows()
	fmt.Println(ups)

	tu.
		WhereEqual("id", id).
		Mod(UserAvatar, "mod-avatar").
		Mod(UserEmail, "mod-email").
		Ups()
	fmt.Println(tu.Rows())

	tu.
		WhereEqual("id", id).
		Mod(UserAvatar, "mod-avatar").
		Mod(UserEmail, "mod-email").
		Ups(map[string]interface{}{UserMobile: "ups-mobile"})
	fmt.Println(tu.Rows())

	// use more map to update
	tu.
		WhereEqual("id", id).
		Ups(
			map[string]interface{}{UserMobile: "ups-mobile", UserAvatar: "ups-avatar"},
			map[string]interface{}{UserEmail: "ups-email", UserAvatar: "ups-avatar", UserTime: times},
			)
	fmt.Println(tu.Rows())

	tu.
		WhereEqual(UserId, id).
		Del()
	dels := tu.Rows()
	fmt.Println(dels)

	tu.
		WhereIn(UserStatus, 90, 91).
		WhereOrBracketsLeft().
		WhereOrEqual(UserId, id-1).
		WhereOrEqual(UserId, id).
		WhereBracketsRight().
		Del()
	fmt.Println(tu.Rows())

	tu.
		WhereLessThanEqual(UserId, 0).
		Del()
	fmt.Println(tu.Rows())

	tu.
		Adds(&User{
			Introduce: "1",
		}, &User{
			Introduce: "2",
		}, &User{
			Introduce: "3",
		})
	fmt.Println(tu.Rows())

	//tu.Print().Del() // unspecified where condition will delete all data
	//fmt.Println(tu.Rows())

	tu.WhereEqual(UserId, 1).Get(&user)
	fmt.Println(user)

	tu.WhereEqual(UserId, -200).Del()
	fmt.Println(tu.Rows())

	// checkout error
	if tu.Error() != nil {
		fmt.Println(tu.Error())
	}

	// transaction
	begin := Begin().Table(&user).Print()
	begin.Add(&User{
		Name: "transaction",
		Time: times,
	})
	id = begin.Id()
	if id == 0 {
		fmt.Println("transaction insert error")
		begin.RollBack()
		return
	}
	fmt.Println(id)

	begin.Add(&User{
		Email: "231231231@gmail.com",
		Time:  times,
	})
	wid := begin.Id()
	fmt.Println(wid)
	if wid == 0 {
		fmt.Println("transaction insert error")
		begin.RollBack()
		return
	}
	begin.WhereEqual(UserId, id).Mod(UserAvatar, "avatar").Ups()
	if begin.Rows() == 0 { // no need to specify the table name again, if your table name doesn't change
		fmt.Println("transaction update error")
		begin.RollBack()
		return
	}
	begin.Table(&User{}).WhereEqual(UserId, wid).Mod(UserNick, "ycm").Ups()
	if begin.rows == 0 {
		fmt.Println("transaction update error")
		begin.RollBack()
		return
	}

	begin.WhereEqual(UserId, wid).Del()
	if begin.Rows() == 0 {
		fmt.Println("transaction delete error")
		begin.RollBack()
		return
	}

	begin.Commit()
	fmt.Println("successful transaction")

	// checkout error
	fmt.Println(begin.Error())

	// select more rows with group and page data
	users := []*User{}
	tu.
		Cols(UserId, UserName , UserAvatar, UserNick).
		WhereMoreThanEqual(UserId, 0).
		Group(UserId).
		Limit(10).
		Page(5).
		Print().
		Asc(UserId).
		Get(&users)
	if tu.Error() != nil {
		fmt.Println(tu.Error())
	}
	for _, v := range users {
		fmt.Println(v.Id, v.Name, v.Avatar, v.Nick)
	}

}
```