"""存储核心：链式版本、丢弃块、失败回滚、崩溃对账、合并数据文件布局。"""

import base64
import json
import os

import pytest

from app.storage import BLOCK_SIZE, BlockStore, ConflictError, StorageError, atomic_write_json

from .conftest import expected_image, sha256_hex


def write_change(index: int, data: bytes) -> dict:
    return {"index": index, "op": "write", "data_base64": base64.b64encode(data).decode("ascii")}


def write_legacy_version(
    data_dir,
    version: int,
    parent_version: int,
    image_size: int,
    base_image: bytes,
    writes: dict[int, bytes],
    discards: tuple[int, ...] = (),
) -> bytes:
    """按旧布局手工落盘一个版本（每块一个独立文件 + 旧格式 meta + current 指针）。

    用于模拟新代码打开旧数据卷的场景；返回该版对应的整盘镜像内容。
    """
    block_count = (image_size + BLOCK_SIZE - 1) // BLOCK_SIZE
    image = bytearray(base_image)
    changes: list[dict] = []
    for index in sorted(writes):
        data = writes[index]
        (data_dir / "blocks" / f"b{index:010d}_v{version:06d}").write_bytes(data)
        changes.append({"index": index, "op": "write"})
        start = index * BLOCK_SIZE
        image[start : start + len(data)] = data
    for index in discards:
        changes.append({"index": index, "op": "discard"})
        start = index * BLOCK_SIZE
        length = min(BLOCK_SIZE, image_size - start)
        image[start : start + length] = b"\x00" * length
    changes.sort(key=lambda c: c["index"])
    meta = {
        "version": version,
        "parent_version": parent_version,
        "image_size": image_size,
        "block_count": block_count,
        "changes": changes,
        "sha256": sha256_hex(bytes(image)),
    }
    atomic_write_json(data_dir / f"v{version:06d}" / "meta.json", meta)
    atomic_write_json(
        data_dir / "current.json",
        {
            "version": version,
            "image_size": image_size,
            "block_count": block_count,
            "sha256": meta["sha256"],
        },
    )
    return bytes(image)


def test_baseline_initialized(store, image_size):
    assert store.current.version == 1
    assert store.current.image_size == image_size
    assert store.current.block_count == 11

    raw = b"".join(store.iter_image())
    assert len(raw) == image_size
    assert sha256_hex(raw) == store.current.sha256 == store.hash_current()
    # 基线文件就是版本 1 的完整内容
    assert raw == open(store.baseline_path, "rb").read()


def test_commit_write_and_inherit(store, image_size):
    payload_v2 = {2: bytes((i * 3) & 0xFF for i in range(BLOCK_SIZE))}
    image_v2 = expected_image(store, payload_v2)

    state_v2 = store.commit(
        parent_version=1,
        changes=[write_change(2, payload_v2[2])],
        expected_sha256=sha256_hex(image_v2),
    )
    assert state_v2.version == 2

    raw = b"".join(store.iter_image())
    assert raw == image_v2
    # 未列出的块沿用父版（抽查块 0、5）
    assert store.read_block(0) == expected_image(store)[0:BLOCK_SIZE]
    assert store.read_block(5) == expected_image(store)[5 * BLOCK_SIZE : 6 * BLOCK_SIZE]
    # 覆盖块生效
    assert store.read_block(2) == payload_v2[2]

    # 第二批只改块 7，块 2 保持 v2 内容
    payload_v3 = {7: b"\xab" * BLOCK_SIZE}
    image_v3 = bytearray(image_v2)
    image_v3[7 * BLOCK_SIZE : 8 * BLOCK_SIZE] = payload_v3[7]
    state_v3 = store.commit(
        parent_version=2,
        changes=[write_change(7, payload_v3[7])],
        expected_sha256=sha256_hex(bytes(image_v3)),
    )
    assert state_v3.version == 3
    assert b"".join(store.iter_image()) == bytes(image_v3)
    assert store.read_block(2) == payload_v2[2]
    assert store.read_block(7) == payload_v3[7]


def test_discard_reads_zero(store, image_size):
    tail_index = store.current.block_count - 1
    tail_len = image_size - tail_index * BLOCK_SIZE  # 777
    state = store.commit(
        parent_version=1,
        changes=[
            {"index": 0, "op": "discard"},
            {"index": tail_index, "op": "discard"},
        ],
        expected_sha256=sha256_hex(
            b"\x00" * BLOCK_SIZE
            + expected_image(store)[BLOCK_SIZE : tail_index * BLOCK_SIZE]
            + b"\x00" * tail_len
        ),
    )
    assert state.version == 2
    assert store.read_block(0) == b"\x00" * BLOCK_SIZE
    # 丢弃的末块按真实长度补零，而不是整块 4 KiB
    assert store.read_block(tail_index) == b"\x00" * tail_len
    full = b"".join(store.iter_image())
    assert len(full) == image_size
    assert full[0:BLOCK_SIZE] == b"\x00" * BLOCK_SIZE
    assert full[-tail_len:] == b"\x00" * tail_len


def test_discard_then_rewrite(store):
    store.commit(
        parent_version=1,
        changes=[{"index": 1, "op": "discard"}],
        expected_sha256=sha256_hex(
            expected_image(store)[:BLOCK_SIZE]
            + b"\x00" * BLOCK_SIZE
            + expected_image(store)[2 * BLOCK_SIZE :]
        ),
    )
    new_data = b"\xcd" * BLOCK_SIZE
    image_v3 = bytearray(expected_image(store))
    image_v3[BLOCK_SIZE : 2 * BLOCK_SIZE] = new_data
    store.commit(
        parent_version=2,
        changes=[write_change(1, new_data)],
        expected_sha256=sha256_hex(bytes(image_v3)),
    )
    assert store.read_block(1) == new_data


def test_stale_parent_rejected(store):
    good = expected_image(store, {0: b"\x11" * BLOCK_SIZE})
    store.commit(
        parent_version=1,
        changes=[write_change(0, b"\x11" * BLOCK_SIZE)],
        expected_sha256=sha256_hex(good),
    )
    with pytest.raises(ConflictError, match="不是当前版本"):
        store.commit(
            parent_version=1,
            changes=[write_change(0, b"\x22" * BLOCK_SIZE)],
            expected_sha256="0" * 64,
        )
    # 当前版本仍是 v2，内容未变
    assert store.current.version == 2
    assert b"".join(store.iter_image()) == good


def test_hash_mismatch_keeps_original(store, image_size):
    before = b"".join(store.iter_image())
    current_version = store.current.version
    with pytest.raises(StorageError, match="摘要不吻合"):
        store.commit(
            parent_version=1,
            changes=[write_change(3, b"\x55" * BLOCK_SIZE)],
            expected_sha256="f" * 64,
        )

    # 版本指针、镜像内容均保持原样
    assert store.current.version == current_version
    assert b"".join(store.iter_image()) == before
    # 失败批次的块数据不得残留（新旧布局目录都干净）
    assert os.listdir(store.root / "blocks") == []
    assert not (store.root / "v000002").exists()
    # 暂存目录被清理
    assert not (store.root / "tmp" / "v000002").exists()


def test_validation_errors(store):
    # 块号越界
    with pytest.raises(StorageError, match="越界"):
        store.commit(1, [{"index": 99, "op": "discard"}], "0" * 64)
    # 重复块号
    with pytest.raises(StorageError, match="重复"):
        store.commit(
            1,
            [
                {"index": 0, "op": "discard"},
                write_change(0, b"\x00" * BLOCK_SIZE),
            ],
            "0" * 64,
        )
    # 非法 base64
    with pytest.raises(StorageError, match="base64"):
        store.commit(1, [{"index": 0, "op": "write", "data_base64": "@@not-base64@@"}], "0" * 64)
    # 块长度不符
    with pytest.raises(StorageError, match="数据长度"):
        store.commit(
            1,
            [write_change(0, b"short")],
            "0" * 64,
        )
    # 非法摘要格式
    with pytest.raises(StorageError, match="SHA-256"):
        store.commit(1, [{"index": 0, "op": "discard"}], "xyz")
    # 非法操作类型
    with pytest.raises(StorageError, match="操作类型非法"):
        store.commit(1, [{"index": 0, "op": "resize"}], "0" * 64)


def test_only_changed_blocks_land_on_disk(store, image_size):
    data = b"\x99" * BLOCK_SIZE
    image = expected_image(store, {4: data})
    store.commit(
        parent_version=1,
        changes=[write_change(4, data)],
        expected_sha256=sha256_hex(image),
    )
    # 只落盘 1 个合并数据文件（4 KiB），没有整盘副本，也不产生旧布局块文件
    data_path = store.root / "v000002" / "data.bin"
    assert data_path.read_bytes() == data
    assert os.listdir(store.root / "blocks") == []
    meta = json.loads((store.root / "v000002" / "meta.json").read_text())
    assert meta["changes"] == [{"index": 4, "op": "write", "offset": 0, "length": BLOCK_SIZE}]
    assert meta["data_file"] == "data.bin"
    assert meta["data_size"] == BLOCK_SIZE
    assert meta["parent_version"] == 1
    # 基线文件大小不变
    assert os.path.getsize(store.baseline_path) == image_size


def test_writes_merged_into_single_data_file(store):
    """一批多个写块合并落盘为单个数据文件，meta 记录每块在文件内的偏移。"""
    payloads = {i: bytes([0x10 + i]) * BLOCK_SIZE for i in (1, 4, 9)}
    image = expected_image(store, payloads)
    state = store.commit(
        parent_version=1,
        changes=[write_change(i, payloads[i]) for i in (9, 1, 4)],  # 乱序提交也应按块号落盘
        expected_sha256=sha256_hex(image),
    )
    assert state.data_files == 1
    # 数据文件内容按块号升序紧密拼接
    data_path = store.root / "v000002" / "data.bin"
    assert data_path.read_bytes() == payloads[1] + payloads[4] + payloads[9]
    assert os.listdir(store.root / "blocks") == []
    meta = json.loads((store.root / "v000002" / "meta.json").read_text())
    assert meta["data_file"] == "data.bin"
    assert meta["data_size"] == 3 * BLOCK_SIZE
    assert meta["changes"] == [
        {"index": 1, "op": "write", "offset": 0, "length": BLOCK_SIZE},
        {"index": 4, "op": "write", "offset": BLOCK_SIZE, "length": BLOCK_SIZE},
        {"index": 9, "op": "write", "offset": 2 * BLOCK_SIZE, "length": BLOCK_SIZE},
    ]
    assert b"".join(store.iter_image()) == image


def test_hundreds_of_writes_land_in_one_file(tmp_path):
    """灾备场景：一批几百个写块只新增 1 个数据文件，而不是几百个小文件。"""
    store = BlockStore(tmp_path / "data")
    store.init_baseline(320 * BLOCK_SIZE)
    payloads = {i: bytes([(i * 7) & 0xFF]) * BLOCK_SIZE for i in range(300)}
    image = expected_image(store, payloads)
    state = store.commit(
        parent_version=1,
        changes=[write_change(i, payloads[i]) for i in range(300)],
        expected_sha256=sha256_hex(image),
    )
    assert state.data_files == 1
    data_path = store.root / "v000002" / "data.bin"
    assert data_path.stat().st_size == 300 * BLOCK_SIZE
    # 数据卷只多出 1 个数据文件（外加 meta 与 current 指针）
    assert os.listdir(store.root / "blocks") == []
    assert b"".join(store.iter_image()) == image
    store.close()


def test_packed_layout_with_tail_block(store, image_size):
    """末块（非整块）写入合并数据文件：按真实长度记录偏移并读回。"""
    tail_index = store.current.block_count - 1
    tail_len = image_size - tail_index * BLOCK_SIZE  # 777
    head = b"\x01" * BLOCK_SIZE
    tail = b"\x02" * tail_len
    image = bytearray(expected_image(store))
    image[0:BLOCK_SIZE] = head
    image[tail_index * BLOCK_SIZE :] = tail
    state = store.commit(
        parent_version=1,
        changes=[write_change(0, head), write_change(tail_index, tail)],
        expected_sha256=sha256_hex(bytes(image)),
    )
    assert state.data_files == 1
    # 数据文件按块号升序紧密拼接：整块 4096 + 末块 777
    data_path = store.root / "v000002" / "data.bin"
    assert data_path.stat().st_size == BLOCK_SIZE + tail_len
    assert data_path.read_bytes() == head + tail
    meta = json.loads((store.root / "v000002" / "meta.json").read_text())
    assert meta["changes"] == [
        {"index": 0, "op": "write", "offset": 0, "length": BLOCK_SIZE},
        {"index": tail_index, "op": "write", "offset": BLOCK_SIZE, "length": tail_len},
    ]
    assert store.read_block(tail_index) == tail
    assert b"".join(store.iter_image()) == bytes(image)


def test_discard_only_batch_skips_data_file(store):
    """纯丢弃批次不产生数据文件。"""
    image = bytearray(expected_image(store))
    image[0:BLOCK_SIZE] = b"\x00" * BLOCK_SIZE
    state = store.commit(
        parent_version=1,
        changes=[{"index": 0, "op": "discard"}],
        expected_sha256=sha256_hex(bytes(image)),
    )
    assert state.data_files == 0
    assert not (store.root / "v000002" / "data.bin").exists()
    meta = json.loads((store.root / "v000002" / "meta.json").read_text())
    assert meta["data_file"] is None
    assert meta["data_size"] == 0
    assert meta["changes"] == [{"index": 0, "op": "discard"}]
    assert b"".join(store.iter_image()) == bytes(image)


def test_restart_serves_last_complete_commit(tmp_path, image_size):
    data_dir = tmp_path / "data"
    s1 = BlockStore(data_dir)
    s1.init_baseline(image_size)
    image_v2 = expected_image(s1, {1: b"\x77" * BLOCK_SIZE})
    s1.commit(
        parent_version=1,
        changes=[write_change(1, b"\x77" * BLOCK_SIZE)],
        expected_sha256=sha256_hex(image_v2),
    )
    s1.close()

    # 模拟崩溃残留：tmp 暂存目录与一个“已写 meta 但 current 未切换”的半成品 v3
    (data_dir / "tmp" / "v000003").mkdir(parents=True)
    (data_dir / "tmp" / "v000003" / "junk").write_bytes(b"x")
    os.makedirs(data_dir / "v000003", exist_ok=True)
    (data_dir / "v000003" / "meta.json").write_text("{broken")

    s2 = BlockStore(data_dir)
    state = s2.open()
    # 重启后只提供最后完整提交的版本 v2
    assert state.version == 2
    assert state.sha256 == sha256_hex(image_v2)
    assert b"".join(s2.iter_image()) == image_v2
    # 崩溃暂存被清理；current 指针保持/回退到 v2
    assert json.loads((data_dir / "current.json").read_text())["version"] == 2
    assert not (data_dir / "tmp" / "v000003").exists()
    s2.close()

    # 再次重启依旧稳定
    s3 = BlockStore(data_dir)
    assert s3.open().version == 2
    s3.close()


def test_restart_with_missing_block_file(tmp_path, image_size):
    """旧布局：缺块文件的版本在重启对账时被截断，回退到最后完整版本。"""
    data_dir = tmp_path / "data"
    s1 = BlockStore(data_dir)
    s1.init_baseline(image_size)
    s1.close()
    base_image = (data_dir / "baseline.raw").read_bytes()
    write_legacy_version(data_dir, 2, 1, image_size, base_image, {0: b"\x33" * BLOCK_SIZE})
    # 人为损坏旧布局 v2：删掉它的块文件
    os.remove(data_dir / "blocks" / "b0000000000_v000002")

    s2 = BlockStore(data_dir)
    state = s2.open()
    # v2 不完整 → 回退到完整的基线 v1
    assert state.version == 1
    assert state.data_files == 0
    assert b"".join(s2.iter_image()) == base_image
    s2.close()


def test_mixed_legacy_and_packed_layout(tmp_path, image_size):
    """旧数据卷原样保留，可直接用新布局续写；同一镜像混读两种布局的块。"""
    data_dir = tmp_path / "data"
    boot = BlockStore(data_dir)
    boot.init_baseline(image_size)
    boot.close()
    base_image = (data_dir / "baseline.raw").read_bytes()

    # 手工构造旧布局 v2：块 2 以逐块文件形式落盘
    legacy_data = bytes((i * 3) & 0xFF for i in range(BLOCK_SIZE))
    image_v2 = write_legacy_version(data_dir, 2, 1, image_size, base_image, {2: legacy_data})

    store = BlockStore(data_dir)
    state = store.open()
    assert state.version == 2
    assert state.data_files == 1  # 旧布局：1 个写块 = 1 个块文件
    assert store.read_block(2) == legacy_data
    assert b"".join(store.iter_image()) == image_v2

    # 新布局续写 v3：覆盖块 5、丢弃块 7
    new_data = b"\xab" * BLOCK_SIZE
    image_v3 = bytearray(image_v2)
    image_v3[5 * BLOCK_SIZE : 6 * BLOCK_SIZE] = new_data
    image_v3[7 * BLOCK_SIZE : 8 * BLOCK_SIZE] = b"\x00" * BLOCK_SIZE
    image_v3 = bytes(image_v3)
    state = store.commit(
        parent_version=2,
        changes=[write_change(5, new_data), {"index": 7, "op": "discard"}],
        expected_sha256=sha256_hex(image_v3),
    )
    assert state.version == 3
    assert state.data_files == 1  # 新布局：本批写块合并为 1 个 data.bin
    # 混读：块 2 来自旧布局块文件，块 5 来自新布局 data.bin，块 7 读零
    assert store.read_block(2) == legacy_data
    assert store.read_block(5) == new_data
    assert store.read_block(7) == b"\x00" * BLOCK_SIZE
    assert b"".join(store.iter_image()) == image_v3
    # 旧布局文件原样保留，新布局只新增 1 个数据文件
    assert (data_dir / "blocks" / "b0000000002_v000002").read_bytes() == legacy_data
    assert (data_dir / "v000003" / "data.bin").read_bytes() == new_data
    store.close()

    # 重启后混读依然正确
    store2 = BlockStore(data_dir)
    assert store2.open().version == 3
    assert store2.read_block(2) == legacy_data
    assert b"".join(store2.iter_image()) == image_v3
    store2.close()


def test_restart_with_missing_data_file(tmp_path, image_size):
    """新布局数据文件缺失：重启回退到最后完整版本。"""
    data_dir = tmp_path / "data"
    s1 = BlockStore(data_dir)
    s1.init_baseline(image_size)
    image_v2 = expected_image(s1, {0: b"\x33" * BLOCK_SIZE})
    s1.commit(
        parent_version=1,
        changes=[write_change(0, b"\x33" * BLOCK_SIZE)],
        expected_sha256=sha256_hex(image_v2),
    )
    s1.close()
    # 人为损坏新布局 v2：删掉合并数据文件
    os.remove(data_dir / "v000002" / "data.bin")

    s2 = BlockStore(data_dir)
    state = s2.open()
    assert state.version == 1
    assert state.data_files == 0
    assert b"".join(s2.iter_image()) == (data_dir / "baseline.raw").read_bytes()
    s2.close()


def test_restart_with_truncated_data_file(tmp_path, image_size):
    """新布局数据文件被截短：版号、文件数、下载内容回退，下一批从该版续提交。"""
    data_dir = tmp_path / "data"
    s1 = BlockStore(data_dir)
    s1.init_baseline(image_size)
    image_v2 = expected_image(s1, {0: b"\x33" * BLOCK_SIZE, 1: b"\x44" * BLOCK_SIZE})
    s1.commit(
        parent_version=1,
        changes=[write_change(0, b"\x33" * BLOCK_SIZE), write_change(1, b"\x44" * BLOCK_SIZE)],
        expected_sha256=sha256_hex(image_v2),
    )
    assert s1.current.data_files == 1
    s1.close()
    # 人为截短新布局数据文件（2 块数据砍掉一半）
    data_path = data_dir / "v000002" / "data.bin"
    assert data_path.stat().st_size == 2 * BLOCK_SIZE
    with open(data_path, "r+b") as fh:
        fh.truncate(BLOCK_SIZE)

    s2 = BlockStore(data_dir)
    state = s2.open()
    # 版号、数据文件数、下载内容都回到最后完整的 v1
    assert state.version == 1
    assert state.data_files == 0
    baseline = (data_dir / "baseline.raw").read_bytes()
    assert b"".join(s2.iter_image()) == baseline
    pointer = json.loads((data_dir / "current.json").read_text())
    assert pointer["version"] == 1

    # 同一版号可用新数据重试：v2 的截短残留被清理，续提交成功
    image_v2b = expected_image(s2, {3: b"\x55" * BLOCK_SIZE})
    state = s2.commit(
        parent_version=1,
        changes=[write_change(3, b"\x55" * BLOCK_SIZE)],
        expected_sha256=sha256_hex(image_v2b),
    )
    assert state.version == 2
    assert state.data_files == 1
    assert b"".join(s2.iter_image()) == image_v2b
    assert (data_dir / "v000002" / "data.bin").stat().st_size == BLOCK_SIZE
    s2.close()

    # 再次重启：续提交的 v2 完整可读
    s3 = BlockStore(data_dir)
    assert s3.open().version == 2
    assert b"".join(s3.iter_image()) == image_v2b
    s3.close()


def test_init_baseline_is_idempotent(store, image_size):
    again = store.init_baseline(image_size)
    assert again.version == 1
    assert again.sha256 == store.current.sha256


def test_failed_commit_then_retry_same_version_succeeds(store):
    """失败回滚后重试同一新版本号，必须读到新数据而非缓存的旧句柄。"""
    data = b"\x66" * BLOCK_SIZE
    target = expected_image(store, {0: data})

    # 第一次：错误摘要失败（哈希过程会把数据文件句柄放进缓存）
    with pytest.raises(StorageError, match="摘要不吻合"):
        store.commit(
            parent_version=1,
            changes=[write_change(0, data)],
            expected_sha256="0" * 64,
        )
    assert store.current.version == 1
    assert os.listdir(store.root / "blocks") == []
    assert not (store.root / "v000002").exists()

    # 第二次：正确摘要，版本号仍是 2，内容与摘要必须正确
    state = store.commit(
        parent_version=1,
        changes=[write_change(0, data)],
        expected_sha256=sha256_hex(target),
    )
    assert state.version == 2
    assert state.data_files == 1
    assert store.read_block(0) == data
    assert b"".join(store.iter_image()) == target


def test_concurrent_reads_during_commits(store):
    """提交线程持续发布新版本的同时，读线程反复整盘读取，不得抛并发异常。"""
    import threading

    errors: list[Exception] = []
    stop = threading.Event()

    def reader():
        try:
            while not stop.is_set():
                # 固定读取视图与对应版本，避免与提交线程的发布动作竞争
                snapshot = store.current
                raw = b"".join(store.iter_image(max_version=snapshot.version))
                assert len(raw) == snapshot.image_size
                assert sha256_hex(raw) == snapshot.sha256
        except Exception as exc:  # noqa: BLE001 - 测试需要捕获任何并发异常
            errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()

    for ver in range(2, 12):
        data = bytes([ver & 0xFF]) * BLOCK_SIZE
        target = bytearray(b"".join(store.iter_image()))
        target[0:BLOCK_SIZE] = data
        store.commit(
            parent_version=ver - 1,
            changes=[write_change(0, data)],
            expected_sha256=sha256_hex(bytes(target)),
        )

    stop.set()
    for t in threads:
        t.join(timeout=10)
    assert errors == []
    assert store.current.version == 11
