# visualize_langgraph.py
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path
from PIL import Image, ImageOps
from app.agent.graph import create_sql_agent_graph

workflow = create_sql_agent_graph()

# Use the built-in API to get the graph spec
graph_spec = workflow.get_graph()  # or graph_spec = workflow.get_graph() depending on version

# Get mermaid or raw spec
# If graph_spec has a method draw_mermaid_png or similar
try:
    # Option A: use the built-in rendering
    img_bytes = graph_spec.draw_mermaid_png()
    # Save the bytes to a file
    with open("langgraph_visual.png", "wb") as f:
        f.write(img_bytes)
    print("✅ Graph image saved at: langgraph_visual.png (via built-in render)")
    exit(0)
except AttributeError:
    # Fallback: extract nodes/edges manually from spec
    raw_graph = graph_spec.graph  # might differ based on version
    nodes = list(raw_graph.nodes)
    edges = list(raw_graph.edges)

# Manual draw fallback
G = nx.DiGraph()
G.add_nodes_from(nodes)
G.add_edges_from(edges)

plt.figure(figsize=(10, 6))
pos = nx.spring_layout(G, seed=42, k=1.0)
nx.draw_networkx_nodes(G, pos, node_size=1500, node_color="lightblue")
nx.draw_networkx_edges(G, pos, arrows=True, arrowstyle="-|>", arrowsize=20)
nx.draw_networkx_labels(G, pos, font_size=9, font_weight="bold")

plt.title("LangGraph: SQL Agent Workflow", fontsize=12)
plt.axis("off")

out_path = Path("langgraph_visual.png")
plt.tight_layout()
plt.savefig(out_path, dpi=200, bbox_inches="tight")
plt.close()

# Optional: add white padding
img = Image.open(out_path)
img = ImageOps.expand(img, border=20, fill="white")
img.save(out_path)

print(f"✅ Graph image saved at: {out_path.resolve()}")
