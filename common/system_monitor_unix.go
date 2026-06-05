//go:build !windows

package common

import (
	"os"

	"golang.org/x/sys/unix"
)

// GetDiskSpaceInfo 获取缓存目录所在磁盘的空间信息 (Unix/Linux/macOS)
func GetDiskSpaceInfo() DiskSpaceInfo {
	cachePath := GetDiskCachePath()
	if cachePath == "" {
		cachePath = os.TempDir()
	}
	return GetDiskSpaceInfoForPath(cachePath)
}

// GetDiskSpaceInfoForPath 获取指定路径所在磁盘的空间信息 (Unix/Linux/macOS)
func GetDiskSpaceInfoForPath(path string) DiskSpaceInfo {
	if path == "" {
		path = os.TempDir()
	}
	info := DiskSpaceInfo{}

	var stat unix.Statfs_t
	err := unix.Statfs(path, &stat)
	if err != nil {
		return info
	}

	// 计算磁盘空间 (显式转换以兼容 FreeBSD，其字段类型为 int64)
	bsize := uint64(stat.Bsize)
	info.Total = uint64(stat.Blocks) * bsize
	info.Free = uint64(stat.Bavail) * bsize
	info.Used = info.Total - uint64(stat.Bfree)*bsize

	if info.Total > 0 {
		info.UsedPercent = float64(info.Used) / float64(info.Total) * 100
	}

	return info
}
