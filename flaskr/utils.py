import copy

# Default colormap
colormap = {
    0: (255, 255, 255, 0.5),                    # below detection
    254: (175, 125, 45, 1),                     # land
    255: (160, 187, 91, 0.21)                   # no data
}

# Colormap colors
rbga = {
    'low': (0, 128, 0, 1),
    'medium': (200, 200, 0, 1),
    'high': (255, 165, 0, 1),
    'vhigh': (255, 0, 0, 1)
}


def get_colormap(low: int = 100, med: int = 140, high: int = 183):
    new_colormap = copy.copy(colormap)
    for i in range(1, low, 1):
        new_colormap[i] = rbga['low']
    for i in range(low + 1, med, 1):
        new_colormap[i] = rbga['medium']
    for i in range(med + 1, high, 1):
        new_colormap[i] = rbga['high']
    for i in range(high, 254, 1):
        new_colormap[i] = rbga['vhigh']
    return new_colormap
