import gradio as gr
import matplotlib.pyplot as plt
import numpy as np


def plot_distribution(mean: float, std: float, num_points: int):
    """
    Plot the normal distribution.

    Args:
        mean (float): The mean of the distribution.
        std (float): The standard deviation of the distribution.
        num_points (int): The number of points to plot.

    Returns:
        matplotlib.figure.Figure: The figure of the plot.
    """
    x = np.linspace(mean - 3 * std, mean + 3 * std, num_points)
    y = (1 / (std * np.sqrt(2 * np.pi))) * np.exp(-0.5 * ((x - mean) / std) ** 2)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(x, y, width=0.5, alpha=0.7, edgecolor="k")
    ax.set_title("Distribution Normale Discrète")
    ax.set_xlabel("Prix")
    ax.set_ylabel("Probabilité")
    return fig


with gr.Blocks() as blocks:
    with gr.Row():
        with gr.Column(scale=1, variant="compact"):
            mean_input = gr.Number(label="Moyenne", value=50)
            std_input = gr.Number(label="Écart-Type", value=10)
            num_points_input = gr.Number(label="Nombre de Points", value=20, step=1)

        with gr.Column(scale=2):
            output_plot = gr.Plot(label="Graphique de Distribution")

    # Button to refresh the graph
    generate_button = gr.Button("Générer")

    # Link between the inputs, and the graph
    generate_button.click(
        fn=plot_distribution,
        inputs=[mean_input, std_input, num_points_input],
        outputs=output_plot,
    )
