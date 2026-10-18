//go:build ios

package main

import "C"

// WailsIOSMain runs main() after UIKit launch (see Wails iOS host).
//
//export WailsIOSMain
func WailsIOSMain() {
	main()
}
