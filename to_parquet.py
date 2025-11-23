from plyfile import PlyData
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pathlib import Path
import xml.etree.ElementTree as ET

def ply_to_parquet(ply_path, parquet_path):
    """Convert a single PLY file to Parquet format"""
    try:
        # Load PLY
        ply = PlyData.read(ply_path)
        vertex_data = ply['vertex'].data

        # Convert to pandas DataFrame
        df = pd.DataFrame(vertex_data)
        
        # Write parquet with compression for Kafka optimization
        df.to_parquet(
            parquet_path, 
            engine="pyarrow",
            compression="snappy",  # Efficient compression for streaming
            index=False
        )
        print(f"✓ Converted: {ply_path} -> {parquet_path}")
        return True
    except Exception as e:
        print(f"✗ Error processing {ply_path}: {str(e)}")
        return False

def parse_classifications(txt_path):
    """Parse classification file to create metadata dataframe"""
    try:
        classifications = {}
        with open(txt_path, 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 2:
                    class_name = ' '.join(parts[:-1])
                    class_id = int(parts[-1])
                    classifications[class_id] = class_name
        return classifications
    except Exception as e:
        print(f"✗ Error parsing classifications: {str(e)}")
        return {}

def export_classifications_parquet(txt_path, parquet_path):
    """Export classification mapping as parquet"""
    try:
        classifications = parse_classifications(txt_path)
        df = pd.DataFrame(list(classifications.items()), columns=['class_id', 'class_name'])
        df.to_parquet(
            parquet_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )
        print(f"✓ Exported classifications: {txt_path} -> {parquet_path}")
        return True
    except Exception as e:
        print(f"✗ Error exporting classifications: {str(e)}")
        return False

def export_color_scale_parquet(xml_path, parquet_path):
    """Export color scale mapping as parquet"""
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Extract color steps
        color_steps = []
        for step in root.findall('.//step'):
            color_steps.append({
                'r': int(step.get('r')),
                'g': int(step.get('g')),
                'b': int(step.get('b')),
                'position': float(step.get('pos'))
            })
        
        df = pd.DataFrame(color_steps)
        df.to_parquet(
            parquet_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )
        print(f"✓ Exported color scale: {xml_path} -> {parquet_path}")
        return True
    except Exception as e:
        print(f"✗ Error exporting color scale: {str(e)}")
        return False

def process_all_ply_files(input_dir, output_dir):
    """Process all PLY files in a directory to Parquet"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all PLY files
    ply_files = list(input_path.glob("**/*.ply"))
    
    if not ply_files:
        print(f"No PLY files found in {input_dir}")
        return False
    
    print(f"Found {len(ply_files)} PLY file(s). Starting conversion...\n")
    
    successful = 0
    for ply_file in ply_files:
        # Create output filename
        output_file = output_path / f"{ply_file.stem}.parquet"
        
        if ply_to_parquet(str(ply_file), str(output_file)):
            successful += 1
    
    # Process metadata files
    print(f"\nProcessing metadata files...")
    
    # Find and export classifications
    txt_files = list(input_path.glob("**/*.txt"))
    for txt_file in txt_files:
        output_file = output_path / f"{txt_file.stem}_metadata.parquet"
        export_classifications_parquet(str(txt_file), str(output_file))
    
    # Find and export color scales
    xml_files = list(input_path.glob("**/*.xml"))
    for xml_file in xml_files:
        output_file = output_path / f"{xml_file.stem}_colorscale.parquet"
        export_color_scale_parquet(str(xml_file), str(output_file))
    
    print(f"\n✓ Successfully converted {successful}/{len(ply_files)} LiDAR file(s)")
    return successful == len(ply_files)

if __name__ == "__main__":
    # Process Toronto_3D LiDAR data
    input_directory = "Toronto_3D"
    output_directory = "parquet_output"
    
    process_all_ply_files(input_directory, output_directory)
    
    print(f"\nParquet files ready for Kafka in: {output_directory}/")
