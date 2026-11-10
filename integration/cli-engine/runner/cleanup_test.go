package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestCleanupTakesOnlyItsRunsResources cleans up run A while run B, or a
// test process started without the runner, holds resources of every kind
// on both hosts: after A ended or was cancelled, A's cleanup unmounts,
// detaches, ends and removes only A's. The commands that change a host are
// fakes that record what they were asked to do.
func TestCleanupTakesOnlyItsRunsResources(t *testing.T) {
	const a, b = "aaaaaaaaaaaa", "bbbbbbbbbbbb"
	fakes := t.TempDir()
	// fusermount3 records its arguments; ssh answers the three listings
	// from files and records every other script it is given.
	for name, script := range map[string]string{
		"fusermount3": "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$FAKES/local\"\n",
		"ssh": "#!/bin/sh\nfor last; do :; done\neval \"set -- $last\"\ncase \"$3\" in\n" +
			"'ps -A -o pid=,command=') cat \"$FAKES/ps\" ;;\n'hdiutil info') cat \"$FAKES/hdiutil\" ;;\nmount) cat \"$FAKES/mount\" ;;\n" +
			"*) printf '%s\\n' \"$3\" >> \"$FAKES/mac\" ;;\nesac\n",
	} {
		if err := os.WriteFile(filepath.Join(fakes, name), []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", fakes+":"+os.Getenv("PATH"))
	t.Setenv("FAKES", fakes)
	aRoot, bRoot := "/Users/m/tetral-test-machines/run-"+a, "/Users/m/tetral-test-machines/run-"+b
	aVolume, bVolume := "/Volumes/tetral-joint-"+a+"-disk9", "/Volumes/tetral-joint-"+b+"-disk8"
	for _, c := range []struct {
		name string
		// cancelled: A's test processes were killed and left what they
		// created.
		cancelled bool
		// standalone: B is a test process started without the runner,
		// whose temporary directories lie directly in the temporary
		// directory.
		standalone bool
	}{
		{"A ended while run B runs", false, false},
		{"A was cancelled while run B runs", true, false},
		{"A was cancelled while a test process without the runner runs", true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, name := range []string{"local", "mac"} {
				_ = os.Remove(filepath.Join(fakes, name))
			}
			temporary, memory := t.TempDir(), t.TempDir()
			aTmp, aMemory := filepath.Join(temporary, "tetral-joint-"+a), filepath.Join(memory, "tetral-joint-"+a)
			bTmp, bMemory := filepath.Join(temporary, "tetral-joint-"+b), filepath.Join(memory, "tetral-joint-"+b)
			if c.standalone {
				bTmp = temporary
			}
			bHomes := []string{filepath.Join(bTmp, "TestB", "001", "m", "home"), filepath.Join(bMemory, "joint-2", "m", "home")}
			aHomes := []string{filepath.Join(aTmp, "TestA", "001", "m", "home"), filepath.Join(aMemory, "joint-1", "m", "home")}
			homes := bHomes
			// The listings hold B's resources, and A's own where A was
			// cancelled.
			ps := "  901 /usr/sbin/sshd -D\n 2201 " + bRoot + "/machine.B1/tetral server run --app b\n 2202 " + bVolume + "/m/tetral server run --app b\n"
			images := "image-path : " + bRoot + "/volume.B2/small.sparseimage\n/dev/disk6\tGUID_partition_scheme\n/dev/disk6s1\tApple_APFS\n" +
				"image-path : ram://2097152\n/dev/disk8\t\n" + "image-path : ram://2097152\n/dev/disk5\t\n"
			mounts := "/dev/disk3s1s1 on / (apfs, sealed, local, read-only, journaled)\n" +
				"/dev/disk6s1 on " + bRoot + "/volume.B2/volume (apfs, local, nodev, nosuid, journaled, noowners)\n" +
				"/dev/disk10s1 on " + bVolume + " (apfs, local, nodev, nosuid, journaled, noowners)\n" +
				"tetral on " + bRoot + "/machine.B1/home/drive (nfs, nodev, nosuid)\n" +
				"tetral on " + bVolume + "/m/home/drive (nfs, nodev, nosuid)\n"
			var wantLocal []string
			wantMac := []string{"rm -rf " + shellQuote(aRoot), "kill 77"}
			if c.cancelled {
				homes = append(homes, aHomes...)
				ps += " 1101 " + aRoot + "/machine.A1/tetral server run --app a\n 1102 " + aVolume + "/m/tetral server run --app a\n"
				images += "image-path : " + aRoot + "/volume.A2/small.sparseimage\n/dev/disk7\tGUID_partition_scheme\n" +
					"image-path : ram://2097152\n/dev/disk9\t\n"
				mounts += "/dev/disk7s1 on " + aRoot + "/volume.A2/volume (apfs, local, nodev, nosuid, journaled, noowners)\n" +
					"/dev/disk11s1 on " + aVolume + " (apfs, local, nodev, nosuid, journaled, noowners)\n" +
					"tetral on " + aRoot + "/machine.A1/home/drive (nfs, nodev, nosuid)\n" +
					"tetral on " + aVolume + "/m/home/drive (nfs, nodev, nosuid)\n"
				for _, home := range aHomes {
					wantLocal = append(wantLocal, "-u -z "+home+"/drive")
				}
				wantMac = append([]string{
					"kill 1101 1102; sleep 1",
					"umount -f " + shellQuote(aRoot+"/volume.A2/volume"),
					"umount -f " + shellQuote(aRoot+"/machine.A1/home/drive"),
					"umount -f " + shellQuote(aVolume+"/m/home/drive"),
					"hdiutil detach -force '/dev/disk7'",
					"hdiutil detach -force '/dev/disk9'",
				}, wantMac...)
			}
			var procMounts strings.Builder
			for _, dir := range append([]string{aTmp, aMemory}, homes...) {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			for _, home := range homes {
				procMounts.WriteString("tetral " + home + "/drive fuse.tetral rw,nosuid,nodev 0 0\n")
			}
			for name, content := range map[string]string{"ps": ps, "hdiutil": images, "mount": mounts} {
				if err := os.WriteFile(filepath.Join(fakes, name), []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			r := &runner{started: time.Now(), logOut: io.Discard, runID: a, tmpRoot: aTmp, memoryRoot: aMemory,
				cfg: config{mac: macHost{destination: "mac"}}, mac: &macLane{root: aRoot, caffeinate: "77"}}
			r.cleanupLocal(context.Background(), procMounts.String())
			r.cleanupMac(context.Background())

			if got := lines(t, filepath.Join(fakes, "local")); !slices.Equal(got, wantLocal) {
				t.Errorf("fusermount3 ran with %q; want %q", got, wantLocal)
			}
			if got := lines(t, filepath.Join(fakes, "mac")); !slices.Equal(got, wantMac) {
				t.Errorf("the Mac ran\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(wantMac, "\n"))
			}
			for _, dir := range bHomes {
				if _, err := os.Stat(dir); err != nil {
					t.Errorf("B's %s: %v", dir, err)
				}
			}
			if _, err := os.Stat(aMemory); !os.IsNotExist(err) {
				t.Errorf("A's in-memory root %s is left: %v", aMemory, err)
			}
			// A's temporary root goes once empty; what a cancelled test
			// process left there stays.
			if _, err := os.Stat(aTmp); os.IsNotExist(err) == c.cancelled {
				t.Errorf("A's temporary root %s: %v; want it left %t", aTmp, err, c.cancelled)
			}
		})
	}
}

func lines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}
