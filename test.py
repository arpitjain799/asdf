import asdf
import zarr

arr = zarr.creation.create((6, 9), chunks=(3, 3))
arr[0:3, 0:3] = 1
arr[0:3, 3:6] = 2
arr[0:3, 6:9] = 3
arr[3:6, 0:3] = 4
arr[3:6, 3:6] = 5
arr[3:6, 6:9] = 6

with asdf.AsdfFile() as af:
    af["arr"] = arr
    af.write_to("test.asdf")

with asdf.open("test.asdf", mode="rw") as af:
    af["arr"][0:3, 0:3] = 0
