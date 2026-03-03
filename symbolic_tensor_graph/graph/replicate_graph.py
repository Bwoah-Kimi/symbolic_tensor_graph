import copy
import typing
import sympy as sp
from .graph import TensorGraph
from ..ops import Slice, BroadcastReduce, Customized


class ReplicateGraph:
    @classmethod
    def _update_tensor_name(cls, graph, tensor_name_template, inplace=False):
        if not inplace:
            graph = copy.deepcopy(graph)
        assert isinstance(graph, TensorGraph)
        tensors = graph.tensors
        for tensor in tensors:
            tensor.name = tensor_name_template % (tensor.name,)
        return graph

    @classmethod
    def _update_tensor_revision(cls, graph, new_revision, inplace=False):
        if not inplace:
            graph = copy.deepcopy(graph)
        assert isinstance(graph, TensorGraph)
        tensors = graph.tensors
        if isinstance(new_revision, str):
            new_revision = lambda old_revision: new_revision
        elif isinstance(new_revision, typing.Callable):
            pass
        else:
            assert False

        for tensor in tensors:
            tensor.revision = new_revision(tensor.revision)
        return graph

    @classmethod
    def _update_symbols(cls, graph, old_symbol_map_new_symbol, inplace=False):
        if not inplace:
            graph = copy.deepcopy(graph)
        assert isinstance(graph, TensorGraph)

        sub_dict = {
            sp.parse_expr(k) if isinstance(k, str) else k: 
            sp.parse_expr(v) if isinstance(v, str) else v
            for k, v in old_symbol_map_new_symbol.items()
        }

        for tensor in graph.tensors:
            if tensor.x1_shape is not None:
                tensor.x1_shape = [dim.subs(sub_dict) for dim in tensor.x1_shape]
            if tensor.x1_hidden is not None:
                tensor.x1_hidden = [dim.subs(sub_dict) for dim in tensor.x1_hidden]
            if tensor.x2_shape is not None:
                tensor.x2_shape = [dim.subs(sub_dict) for dim in tensor.x2_shape]
            if tensor.x2_hidden is not None:
                tensor.x2_hidden = [dim.subs(sub_dict) for dim in tensor.x2_hidden]

            if tensor.op_attr is None:
                continue

            if tensor.op_type in {Slice.type_name, BroadcastReduce.type_name, Customized.type_name}:
                # --- 这是修改的核心 ---
                # 使用 if/else 结构确保特殊处理和通用处理是互斥的
                if tensor.op_type == BroadcastReduce.type_name and "*" in tensor.op_attr:
                    parts = tensor.op_attr.split("*", 1)
                    if len(parts) == 2:
                        axis_part, amplifier_part = parts
                        try:
                            amplifier_expr = sp.parse_expr(amplifier_part)
                            new_amplifier_expr = amplifier_expr.subs(sub_dict)
                            tensor.op_attr = f"{axis_part}*{str(new_amplifier_expr)}"
                        except (sp.SympifyError, SyntaxError):
                            new_amplifier_str = amplifier_part
                            for from_, to_ in sub_dict.items():
                                new_amplifier_str = new_amplifier_str.replace(str(from_), f"({str(to_)})")
                            tensor.op_attr = f"{axis_part}*{new_amplifier_str}"
                    # 如果 split 失败, 会自动落入下面的 else 块，这是正确的行为
                else:
                    # 通用处理逻辑，适用于 Slice, Customized, 以及不符合 "axis*amp" 格式的 BroadcastReduce
                    try:
                        expr = sp.parse_expr(tensor.op_attr)
                        new_expr = expr.subs(sub_dict)
                        tensor.op_attr = str(new_expr)
                    except (sp.SympifyError, SyntaxError):
                        attr_str = tensor.op_attr
                        for from_, to_ in sub_dict.items():
                            attr_str = attr_str.replace(str(from_), f"({str(to_)})")
                        tensor.op_attr = attr_str
                # --- 修改结束 ---
                    
        return graph

    @classmethod
    def apply(
        cls,
        graph,
        tensor_name_template=None,
        new_revision=None,
        old_symbol_map_new_symbol=None,
        inplace=False,
    ):
        if not inplace:
            graph = copy.deepcopy(graph)
        if not tensor_name_template is None:
            cls._update_tensor_name(graph, tensor_name_template, inplace=True)
        if not new_revision is None:
            cls._update_tensor_revision(graph, new_revision, inplace=True)
        if not old_symbol_map_new_symbol is None:
            cls._update_symbols(graph, old_symbol_map_new_symbol, inplace=True)
        return graph
