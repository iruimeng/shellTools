package main

import (
	//"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func myTree(dirname string, curLevel int, hierMap map[int]bool) error {

	dirAbs, err := filepath.Abs(dirname)
	//fmt.Println(dirAbs)
	if err != nil {
		return err
	}
	if Exist(dirAbs) == false {
		fmt.Println("No such file or directory")
		return nil
	}
	fileInfo, err := ioutil.ReadDir(dirAbs)
	if err != nil {
		return err
	}

	fileNum := len(fileInfo)
	for i, finfo := range fileInfo {
		//fmt.Println(finfo.Name())
		for j := 1; j < curLevel; j++ {
			if hierMap[j] {
				fmt.Print("|")
			} else {
				fmt.Print(" ")
			}
			fmt.Print(strings.Repeat(" ", 3))
		}

		tmpMap := map[int]bool{}
		for k, v := range hierMap {
			tmpMap[k] = v
		}
		if i+1 == fileNum {
			fmt.Print("`")
			delete(tmpMap, curLevel)
		} else {
			fmt.Print("|")
			tmpMap[curLevel] = true
		}
		fmt.Print("-- ")
		fmt.Println(finfo.Name())
		if finfo.IsDir() {
			myTree(filepath.Join(dirAbs, finfo.Name()), curLevel+1, tmpMap)
		}
	}
	return nil
}

func substr(s string, pos, length int) string {
	runes := []rune(s)
	l := pos + length
	if l > len(runes) {
		l = len(runes)
	}
	return string(runes[pos:l])
}

func main() {
	if len(os.Args) > 1 {
		myTree(os.Args[1], 1, map[int]bool{1: true})
	} else {
		file, _ := exec.LookPath(os.Args[0])
		path, _ := filepath.Abs(file)
		ppath := substr(path, 0, strings.LastIndex(path, "/"))
		myTree(ppath, 1, map[int]bool{1: true})
	}
}
