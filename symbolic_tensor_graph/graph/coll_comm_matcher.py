import copy
import sympy as sp


class CommunicationMatcher:

    class EndType:
        PARTITION = "partition"
        REDUCED = "reduced"

    class CommType:
        ALL_GATHER = "all_gather"
        ALL_REDUCE = "all_reduce"
        ALL_TO_ALL = "all_to_all"
        REDUCE_SCATTER = "reduce_scatter"

    @classmethod
    def match_comms(
        cls, from_shape, from_hidden, to_shape, to_hidden, parallel_symbols
    ):
        from_parallel_dims = cls.get_parallel_dims(
            from_shape, from_hidden, parallel_symbols
        )
        to_parallel_dims = cls.get_parallel_dims(to_shape, to_hidden, parallel_symbols)
        matched_comm_pair = list()
        for parallel_symbol in parallel_symbols:
            if (not parallel_symbol in from_parallel_dims.keys()) and (
                not parallel_symbol in to_parallel_dims.keys()
            ):
                continue
            if parallel_symbol in from_parallel_dims.keys():
                from_comm = from_parallel_dims[parallel_symbol]
            else:
                assert False
            if parallel_symbol in to_parallel_dims.keys():
                to_comm = to_parallel_dims[parallel_symbol]
            else:
                to_comm = (cls.EndType.REDUCED, None)
            matched_comm_pair.append((from_comm, to_comm, parallel_symbol))
        comms = list()
        for from_comm, to_comm, parallel_symbol in matched_comm_pair:
            if from_comm[0] == cls.EndType.PARTITION:
                if to_comm[0] == cls.EndType.PARTITION:
                    if not from_comm[1] == to_comm[1]:
                        # TODO: need some clever way to examine if there two dims contains each other and no a2a required
                        comms.append(
                            (
                                cls.CommType.ALL_TO_ALL,
                                from_comm[1],
                                to_comm[1],
                                parallel_symbol,
                            )
                        )
                    else:
                        # do nothing
                        pass
                elif to_comm[0] == cls.EndType.REDUCED:
                    assert to_comm[1] is None  # shouldnt dim on reduced dim
                    comms.append(
                        (
                            cls.CommType.ALL_GATHER,
                            from_comm[1],
                            to_comm[1],
                            parallel_symbol,
                        )
                    )
                else:
                    assert False
            elif from_comm[0] == cls.EndType.REDUCED:
                if to_comm[0] == cls.EndType.PARTITION:
                    comms.append(
                        (
                            cls.CommType.REDUCE_SCATTER,
                            from_comm[1],
                            to_comm[1],
                            parallel_symbol,
                        )
                    )
                elif to_comm[0] == cls.EndType.REDUCED:
                    if to_comm[1] is None:
                        comms.append(
                            (
                                cls.CommType.ALL_REDUCE,
                                from_comm[1],
                                to_comm[1],
                                parallel_symbol,
                            )
                        )
                    else:
                        # do nothing
                        pass
                else:
                    assert False
            else:
                assert False
        
        return comms

    @classmethod
    def get_parallel_dims(cls, shape, hidden, parallel_symbols):
        remaining_parallel_symbols = copy.deepcopy(parallel_symbols)

        parallel_dims = dict()

        for dim in shape:
            if isinstance(dim, int) or isinstance(dim, float):
                continue
            matched = None
            for parallel_symbol in remaining_parallel_symbols:
                if parallel_symbol in dim.free_symbols:
                    matched = parallel_symbol
                    break
            if not matched is None:
                remaining_parallel_symbols.remove(matched)
                parallel_dims[matched] = cls.EndType.PARTITION, dim

        for dim in hidden:
            if isinstance(dim, int) or isinstance(dim, float):
                continue
            matched = None
            for parallel_symbol in remaining_parallel_symbols:
                if parallel_symbol in dim.free_symbols:
                    matched = parallel_symbol
                    break
            if not matched is None:
                remaining_parallel_symbols.remove(matched)
                parallel_dims[matched] = cls.EndType.REDUCED, dim

        assert len(parallel_dims) + len(remaining_parallel_symbols) == len(
            parallel_symbols
        )
        return parallel_dims


class CommunicationMatcherV2:
    class EndType:
        DUPLICATED = "duplicated"
        PARTITIONED = "partitioned"
        PARTIALSUM = "partialsum"

    class CommType:
        ALL_GATHER = "all_gather"
        ALL_REDUCE = "all_reduce"
        ALL_TO_ALL = "all_to_all"
        REDUCE_SCATTER = "reduce_scatter"
        SLICED = "sliced"  # virtual comm type, not really a coll communication
        GATHER_SCATTER = (
            "gather_scatter"  # virtual comm type, general case of a2a/Identity
        )
        IDENTITY = "identity"  # virtual comm type, doing nothing

    @classmethod
    def get_parallel_dims(cls, shape, hidden, parallel_symbols):
        """
        Determines how tensor dimensions are affected by parallelization symbols.
        This version uses sympy.subs() for efficient, one-shot substitution.
        """
        # 使用集合进行高效的查找和操作
        all_parallel_symbols = set(parallel_symbols)
        
        # 1. 默认所有并行维度都是 DUPLICATED
        parallel_dims = {
            symbol: (cls.EndType.DUPLICATED, None)
            for symbol in all_parallel_symbols
        }

        # 使用一个集合来跟踪已经分配了角色的并行符号（Partitioned 或 PartialSum）
        processed_symbols = set()

        # 2. 遍历 shape 寻找 PARTITIONED 的维度
        for dim in shape:
            # 跳过数字，只处理 sympy 表达式
            if not hasattr(dim, 'free_symbols'):
                continue
            
            # 找出当前维度中包含的所有并行符号
            dim_parallel_symbols = dim.free_symbols.intersection(all_parallel_symbols)

            for matched_symbol in dim_parallel_symbols:
                # 如果这个符号已经被处理过，就跳过（每个符号只处理一次）
                if matched_symbol in processed_symbols:
                    continue

                # 创建一个替换字典，将所有 *其他* 并行符号替换为 1
                sub_dict = {
                    s: 1 for s in all_parallel_symbols if s != matched_symbol
                }
                
                # 使用 .subs() 一次性完成所有替换
                isolated_dim = dim.subs(sub_dict)
                
                parallel_dims[matched_symbol] = (cls.EndType.PARTITIONED, isolated_dim)
                processed_symbols.add(matched_symbol)
        
        # 3. 遍历 hidden 寻找 PARTIALSUM 的维度
        for dim in hidden:
            if not hasattr(dim, 'free_symbols'):
                continue

            dim_parallel_symbols = dim.free_symbols.intersection(all_parallel_symbols)

            for matched_symbol in dim_parallel_symbols:
                # 如果符号已被处理（例如，已经在 shape 中被标记为 PARTITIONED），则跳过
                if matched_symbol in processed_symbols:
                    continue

                # 同样，创建替换字典并进行替换
                sub_dict = {
                    s: 1 for s in all_parallel_symbols if s != matched_symbol
                }
                isolated_dim = dim.subs(sub_dict)
                
                parallel_dims[matched_symbol] = (cls.EndType.PARTIALSUM, isolated_dim)
                processed_symbols.add(matched_symbol)

        return parallel_dims
    # def get_parallel_dims(cls, shape, hidden, parallel_symbols):
    #     remaining_parallel_symbols = copy.deepcopy(parallel_symbols)
    #     parallel_symbols = sp.symbols("dp tp cp ep")

    #     parallel_dims = dict()
    #     for dim in shape:
    #         if isinstance(dim, int) or isinstance(dim, float):
    #             continue
    #         while True:
    #             matched = None
    #             for parallel_symbol in remaining_parallel_symbols:
    #                 # if parallel_symbol == sp.symbols("ep"):
    #                 #     pass
    #                 if parallel_symbol in dim.free_symbols:
    #                     matched = parallel_symbol
    #                     break
    #             if not matched is None:
    #                 dim2 = copy.deepcopy(dim)
    #                 # if dim2 == sp.parse_expr("Seq/(cp*tp)"):
    #                 #     pass
    #                 for symbol in parallel_symbols:
    #                     if symbol == matched:
    #                         continue
    #                     if symbol in dim2.free_symbols:
    #                         dim2 = dim2.replace(symbol, 1)
    #                 remaining_parallel_symbols.remove(matched)
    #                 parallel_dims[matched] = cls.EndType.PARTITIONED, dim2
    #             else:
    #                 break

    #     for dim in hidden:
    #         if isinstance(dim, int) or isinstance(dim, float):
    #             continue
    #         while True:
    #             matched = None
    #             for parallel_symbol in remaining_parallel_symbols:
    #                 if parallel_symbol in dim.free_symbols:
    #                     matched = parallel_symbol
    #                     break
    #             if not matched is None:
    #                 dim2 = copy.deepcopy(dim)
    #                 for symbol in parallel_symbols:
    #                     if symbol == matched:
    #                         continue
    #                     if symbol in dim2.free_symbols:
    #                         dim2 = dim2.replace(symbol, 1)
    #                 remaining_parallel_symbols.remove(matched)
    #                 parallel_dims[matched] = cls.EndType.PARTIALSUM, dim2
    #             else:
    #                 break

    #     for symbol in remaining_parallel_symbols:
    #         parallel_dims[symbol] = cls.EndType.DUPLICATED, None

    #     return parallel_dims

    @classmethod
    def match_comms(
        cls, from_shape, from_hidden, to_shape, to_hidden, parallel_symbols
    ):
        from_parallel_dims = cls.get_parallel_dims(
            from_shape, from_hidden, parallel_symbols
        )
        to_parallel_dims = cls.get_parallel_dims(to_shape, to_hidden, parallel_symbols)
        matched_comm_pair = list()
        for parallel_symbol in parallel_symbols:
            from_parallel_dim = from_parallel_dims[parallel_symbol]
            to_parallel_dim = to_parallel_dims[parallel_symbol]
            matched_comm_pair.append(
                (from_parallel_dim, to_parallel_dim, parallel_symbol)
            )

        comms = list()
        for from_comm, to_comm, parallel_symbol in matched_comm_pair:
            if to_comm[0] == cls.EndType.PARTIALSUM:
                if from_comm[0] == cls.EndType.PARTIALSUM:
                    # no change
                    comms.append(
                        (
                            cls.CommType.IDENTITY,
                            from_comm[1],
                            to_comm[1],
                            parallel_symbol,
                        )
                    )
                else:
                    assert False, "cannot produce partialsum from non-partialsum"
            elif to_comm[0] == cls.EndType.PARTITIONED:
                if from_comm[0] == cls.EndType.DUPLICATED:
                    # from duplicated to partition, slices
                    comms.append(
                        (cls.CommType.SLICED, from_comm[1], to_comm[1], parallel_symbol)
                    )
                elif from_comm[0] == cls.EndType.PARTITIONED:
                    comms.append(
                        (
                            cls.CommType.GATHER_SCATTER,
                            from_comm[1],
                            to_comm[1],
                            parallel_symbol,
                        )
                    )
                elif from_comm[0] == cls.EndType.PARTIALSUM:
                    # partialsum to partition
                    comms.append(
                        (
                            cls.CommType.REDUCE_SCATTER,
                            from_comm[1],
                            to_comm[1],
                            parallel_symbol,
                        )
                    )
                else:
                    assert False
            elif to_comm[0] == cls.EndType.DUPLICATED:
                if from_comm[0] == cls.EndType.DUPLICATED:
                    comms.append(
                        (
                            cls.CommType.IDENTITY,
                            from_comm[1],
                            to_comm[1],
                            parallel_symbol,
                        )
                    )
                elif from_comm[0] == cls.EndType.PARTITIONED:
                    comms.append(
                        (
                            cls.CommType.ALL_GATHER,
                            from_comm[1],
                            to_comm[1],
                            parallel_symbol,
                        )
                    )
                elif from_comm[0] == cls.EndType.PARTIALSUM:
                    comms.append(
                        (
                            cls.CommType.ALL_REDUCE,
                            from_comm[1],
                            to_comm[1],
                            parallel_symbol,
                        )
                    )
                else:
                    assert False
            else:
                assert False

        # special case handling
        for i, comm in enumerate(comms):
            if comm[0] == cls.CommType.GATHER_SCATTER:
                from_dim = comm[1]
                to_dim = comm[2]
                if from_dim == to_dim:
                    comm = (cls.CommType.IDENTITY,) + comm[1:]
                else:
                    comm = (cls.CommType.ALL_TO_ALL,) + comm[1:]
                comms[i] = comm

        def _filter_fn(comm):
            if comm[0] in {cls.CommType.SLICED}:
                print(
                    f"unefficient collective {comm[0]} found! check if the sharding plan is reasonable."
                )
            return comm[0] not in {cls.CommType.IDENTITY, cls.CommType.SLICED}

        filtered_comms = filter(
            _filter_fn,
            comms,
        )
        return filtered_comms
