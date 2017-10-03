
def color_position_negative(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for negative
    strings, black otherwise.
    """
    color = 'darkred' if val < 0 else 'darkblue'
    return 'color: %s' % color

def highlight_max(s, color='background-color: lightgreen'):
    '''
    highlight the maximum in a Series yellow.
    '''
    is_max = s == s.max()
    return [color if v else '' for v in is_max]

def highlight_min(s, color='background-color: lightgreen'):
    '''
    highlight the minimum in a Series yellow.
    '''
    is_min = s == s.min()
    return [color if v else '' for v in is_min]    
    