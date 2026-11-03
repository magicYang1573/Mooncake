#!/usr/bin/env python3
"""Verify local Engram row assembly above the signed-int32 byte boundary."""

import argparse

import torch
import triton

from mooncake._engram import kernels


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", type=int, default=0)
    args = parser.parse_args()

    torch.cuda.set_device(args.device)
    width = stride = 264
    row_index = 8_200_000
    slot_rows = 10_000_000
    byte_offset = row_index * stride
    assert byte_offset > 2**31

    local = torch.empty(byte_offset + stride, dtype=torch.uint8, device="cuda")
    expected = torch.arange(width, dtype=torch.int64, device="cuda").to(torch.uint8)
    local[byte_offset : byte_offset + width] = expected
    mapped = torch.tensor([row_index], dtype=torch.int32, device="cuda")
    fetched = torch.zeros((1, stride), dtype=torch.uint8, device="cuda")
    output = torch.zeros((1, 1, width), dtype=torch.uint8, device="cuda")

    kernels.finish[(1,)](
        mapped,
        local.data_ptr(),
        fetched,
        output,
        1,
        1,
        width,
        stride,
        slot_rows,
        0,
        *output.stride()[:2],
        triton.next_power_of_2(width),
    )
    torch.cuda.synchronize()
    torch.testing.assert_close(output[0, 0], expected, rtol=0, atol=0)
    print(f"large-offset finish passed: byte_offset={byte_offset}")


if __name__ == "__main__":
    main()
