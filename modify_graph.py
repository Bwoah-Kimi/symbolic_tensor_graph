import argparse
import os
import glob
import sys
import importlib.util

# Assuming the script is run from a location where 'chakra' is in the Python path.
# If not, you may need to adjust sys.path.
try:
    from chakra.schema.protobuf import et_def_pb2
    from chakra.src.third_party.utils.protolib import openFileRd, decodeMessage, encodeMessage
except ImportError:
    print("Error: Failed to import Chakra modules.")
    print("Please ensure that the Chakra library is installed and accessible in your PYTHONPATH.")
    sys.exit(1)

def modify_et_file(input_path, output_path):
    """
    Reads an .et file, modifies the num_ops of specified compute nodes to 0,
    and writes the result to a new .et file.
    """
    try:
        # Open the source and destination files
        et_in = openFileRd(input_path)
        # protolib does not have a writer, use standard python open
        et_out = open(output_path, "wb")
        print(f"Processing '{os.path.basename(input_path)}' -> '{os.path.basename(output_path)}'")

        # First, attempt to read and write the GlobalMetadata
        gm = et_def_pb2.GlobalMetadata()
        if decodeMessage(et_in, gm):
            encodeMessage(et_out, gm)

        # Process each node in the graph
        node = et_def_pb2.Node()
        nodes_processed = 0
        nodes_modified = 0
        while decodeMessage(et_in, node):
            nodes_processed += 1
            
            # Check if the node is a computation node and its name matches our criteria
            is_comp_node = hasattr(node, 'type') and node.type == 4
            
            if is_comp_node:
                node_name = node.name
                is_target_layer = "in_emb" in node_name or "out_emb" in node_name or "loss" in node_name
                
                if is_target_layer:
                    # It's a target node, find and modify its num_ops attribute.
                    modified = False
                    for attr in node.attr:
                        if attr.name == "num_ops":
                            if attr.int64_val != 0:
                                attr.int64_val = 100
                                nodes_modified += 1
                                modified = True
                            break
                    # if modified:
                    #     print(f"  -> Modified 'num_ops' for node: {node.name}")

            # Write the (possibly modified) node to the output file
            encodeMessage(et_out, node)

        et_in.close()
        et_out.close()
        print(f"Finished processing. Total nodes: {nodes_processed}, Modified nodes: {nodes_modified}\n")

    except Exception as e:
        print(f"An error occurred while processing {input_path}: {e}")
        import traceback
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(
        description="Modifies num_ops for specified layers in Chakra .et files."
    )
    parser.add_argument(
        "--input_dir", 
        type=str, 
        required=True, 
        help="Directory containing the original .et files."
    )
    parser.add_argument(
        "--output_dir", 
        type=str, 
        required=True, 
        help="Directory where the modified .et files will be saved."
    )
    args = parser.parse_args()

    # Ensure the output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Find all .et files in the input directory
    et_files = glob.glob(os.path.join(args.input_dir, '*.et'))

    if not et_files:
        print(f"No .et files found in '{args.input_dir}'.")
        return

    print(f"Found {len(et_files)} .et files to process.")

    # Process each file
    for file_path in et_files:
        base_filename = os.path.basename(file_path)
        output_file_path = os.path.join(args.output_dir, base_filename)
        modify_et_file(file_path, output_file_path)

    print("All files processed successfully.")

if __name__ == "__main__":
    main()

