package query

import "gorm.io/gen"

// Paginate 返回一个 Scope 函数
// page: 当前页码 (从1开始)
// pageSize: 每页大小
func Paginate(page int, pageSize int) func(dao gen.Dao) gen.Dao {
	return func(dao gen.Dao) gen.Dao {
		if page <= 0 {
			page = 1
		}
		switch {
		case pageSize > 100:
			pageSize = 100
		case pageSize <= 0:
			pageSize = 10
		}

		offset := (page - 1) * pageSize
		return dao.Limit(pageSize).Offset(offset)
	}
}
