import graphviz
from functools import partial

def box_label(key, verbose=False):
    """Label boxes in graph by chunk index

    >>> box_label(('x', 1, 2, 3))
    '(1, 2, 3)'
    >>> box_label(('x', 123))
    '123'
    >>> box_label('x')
    ''
    """
    if isinstance(key, tuple):
        key = key[1:]
        if len(key) == 1:
            [key] = key
        return str(key)
    elif verbose:
        return str(key)
    else:
        return ""

def ishashable(x):
    """Is x hashable?

    Examples
    --------

    >>> ishashable(1)
    True
    >>> ishashable([1])
    False
    """
    try:
        hash(x)
        return True
    except TypeError:
        return False


def istask(x):
    """Is x a runnable task?

    A task is a tuple with a callable first argument

    Examples
    --------

    >>> inc = lambda x: x + 1
    >>> istask((inc, 1))
    True
    >>> istask(1)
    False
    """
    return type(x) is tuple and x and callable(x[0])

def name(x):
    try:
        return str(hash(x))
    except TypeError:
        return str(hash(str(x)))
def to_graphviz(
    dsk,
    data_attributes=None,
    function_attributes=None,
    rankdir="BT",
    graph_attr=None,
    node_attr=None,
    edge_attr=None,
    collapse_outputs=False,
    verbose=False,
    **kwargs,
):
    data_attributes = data_attributes or {}
    function_attributes = function_attributes or {}
    graph_attr = graph_attr or {}
    node_attr = node_attr or {}
    edge_attr = edge_attr or {}

    graph_attr["rankdir"] = rankdir
    node_attr["fontname"] = "helvetica"

    graph_attr.update(kwargs)
    g = graphviz.Digraph(
        graph_attr=graph_attr, node_attr=node_attr, edge_attr=edge_attr
    )

    seen = set()
    connected = set()

    for k, v in dsk.items():
        k_name = name(k)
        if istask(v):
            func_name = name((k, "function")) if not collapse_outputs else k_name
            if collapse_outputs or func_name not in seen:
                seen.add(func_name)
                attrs = function_attributes.get(k, {}).copy()
                attrs.setdefault("label", key_split(k))
                attrs.setdefault("shape", "circle")
                g.node(func_name, **attrs)
            if not collapse_outputs:
                g.edge(func_name, k_name)
                connected.add(func_name)
                connected.add(k_name)

            for dep in get_dependencies(dsk, k):
                dep_name = name(dep)
                if dep_name not in seen:
                    seen.add(dep_name)
                    attrs = data_attributes.get(dep, {}).copy()
                    attrs.setdefault("label", box_label(dep, verbose))
                    attrs.setdefault("shape", "box")
                    g.node(dep_name, **attrs)
                g.edge(dep_name, func_name)
                connected.add(dep_name)
                connected.add(func_name)

        elif ishashable(v) and v in dsk:
            v_name = name(v)
            g.edge(v_name, k_name)
            connected.add(v_name)
            connected.add(k_name)

        if (not collapse_outputs or k_name in connected) and k_name not in seen:
            seen.add(k_name)
            attrs = data_attributes.get(k, {}).copy()
            attrs.setdefault("label", box_label(k, verbose))
            attrs.setdefault("shape", "box")
            g.node(k_name, **attrs)
    return g


IPYTHON_IMAGE_FORMATS = frozenset(["jpeg", "png"])
IPYTHON_NO_DISPLAY_FORMATS = frozenset(["dot", "pdf"])

def dot_graph(dsk, filename="mydask", format=None, **kwargs):
    """
    Render a task graph using dot.

    If `filename` is not None, write a file to disk with the specified name and extension.
    If no extension is specified, '.png' will be used by default.

    Parameters
    ----------
    dsk : dict
        The graph to display.
    filename : str or None, optional
        The name of the file to write to disk. If the provided `filename`
        doesn't include an extension, '.png' will be used by default.
        If `filename` is None, no file will be written, and we communicate
        with dot using only pipes.  Default is 'mydask'.
    format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
        Format in which to write output file.  Default is 'png'.
    **kwargs
        Additional keyword arguments to forward to `to_graphviz`.

    Returns
    -------
    result : None or IPython.display.Image or IPython.display.SVG  (See below.)

    Notes
    -----
    If IPython is installed, we return an IPython.display object in the
    requested format.  If IPython is not installed, we just return None.

    We always return None if format is 'pdf' or 'dot', because IPython can't
    display these formats natively. Passing these formats with filename=None
    will not produce any useful output.

    See Also
    --------
    dask.dot.to_graphviz
    """
    g = to_graphviz(dsk, **kwargs)
    return graphviz_to_file(g, filename, format)


def graphviz_to_file(g, filename, format):
    fmts = [".png", ".pdf", ".dot", ".svg", ".jpeg", ".jpg"]

    if (
        format is None
        and filename is not None
        and any(filename.lower().endswith(fmt) for fmt in fmts)
    ):
        filename, format = os.path.splitext(filename)
        format = format[1:].lower()

    if format is None:
        format = "png"

    data = g.pipe(format=format)
    if not data:
        raise RuntimeError(
            "Graphviz failed to properly produce an image. "
            "This probably means your installation of graphviz "
            "is missing png support. See: "
            "https://github.com/ContinuumIO/anaconda-issues/"
            "issues/485 for more information."
        )

    display_cls = _get_display_cls(format)

    if filename is None:
        return display_cls(data=data)

    full_filename = ".".join([filename, format])
    with open(full_filename, "wb") as f:
        f.write(data)

    return display_cls(filename=full_filename)



def _get_display_cls(format):
    """
    Get the appropriate IPython display class for `format`.

    Returns `IPython.display.SVG` if format=='svg', otherwise
    `IPython.display.Image`.

    If IPython is not importable, return dummy function that swallows its
    arguments and returns None.
    """
    dummy = lambda *args, **kwargs: None
    try:
        import IPython.display as display
    except ImportError:
        # Can't return a display object if no IPython.
        return dummy

    if format in IPYTHON_NO_DISPLAY_FORMATS:
        # IPython can't display this format natively, so just return None.
        return dummy
    elif format in IPYTHON_IMAGE_FORMATS:
        # Partially apply `format` so that `Image` and `SVG` supply a uniform
        # interface to the caller.
        return partial(display.Image, format=format)
    elif format == "svg":
        return display.SVG
    else:
        raise ValueError("Unknown format '%s' passed to `dot_graph`" % format)
        

def key_split(s):
    """
    >>> key_split('x')
    'x'
    >>> key_split('x-1')
    'x'
    >>> key_split('x-1-2-3')
    'x'
    >>> key_split(('x-2', 1))
    'x'
    >>> key_split("('x-2', 1)")
    'x'
    >>> key_split('hello-world-1')
    'hello-world'
    >>> key_split(b'hello-world-1')
    'hello-world'
    >>> key_split('ae05086432ca935f6eba409a8ecd4896')
    'data'
    >>> key_split('<module.submodule.myclass object at 0xdaf372')
    'myclass'
    >>> key_split(None)
    'Other'
    >>> key_split('x-abcdefab')  # ignores hex
    'x'
    >>> key_split('_(x)')  # strips unpleasant characters
    'x'
    """
    if type(s) is bytes:
        s = s.decode()
    if type(s) is tuple:
        s = s[0]
    try:
        words = s.split("-")
        if not words[0][0].isalpha():
            result = words[0].strip("_'()\"")
        else:
            result = words[0]
        for word in words[1:]:
            if word.isalpha() and not (
                len(word) == 8 and hex_pattern.match(word) is not None
            ):
                result += "-" + word
            else:
                break
        if len(result) == 32 and re.match(r"[a-f0-9]{32}", result):
            return "data"
        else:
            if result[0] == "<":
                result = result.strip("<>").split()[0].split(".")[-1]
            return result
    except Exception:
        return "Other"

    
def get_dependencies(dsk, key=None, task="__no_default__", as_list=False):
    """Get the immediate tasks on which this task depends

    Examples
    --------
    >>> dsk = {'x': 1,
    ...        'y': (inc, 'x'),
    ...        'z': (add, 'x', 'y'),
    ...        'w': (inc, 'z'),
    ...        'a': (add, (inc, 'x'), 1)}

    >>> get_dependencies(dsk, 'x')
    set()

    >>> get_dependencies(dsk, 'y')
    {'x'}

    >>> get_dependencies(dsk, 'z')  # doctest: +SKIP
    {'x', 'y'}

    >>> get_dependencies(dsk, 'w')  # Only direct dependencies
    {'z'}

    >>> get_dependencies(dsk, 'a')  # Ignore non-keys
    {'x'}

    >>> get_dependencies(dsk, task=(inc, 'x'))  # provide tasks directly
    {'x'}
    """
    if key is not None:
        arg = dsk[key]
    elif task is not no_default:
        arg = task
    else:
        raise ValueError("Provide either key or task")

    return keys_in_tasks(dsk, [arg], as_list=as_list)

def keys_in_tasks(keys, tasks, as_list=False):
    """Returns the keys in `keys` that are also in `tasks`

    Examples
    --------
    >>> dsk = {'x': 1,
    ...        'y': (inc, 'x'),
    ...        'z': (add, 'x', 'y'),
    ...        'w': (inc, 'z'),
    ...        'a': (add, (inc, 'x'), 1)}

    >>> keys_in_tasks(dsk, ['x', 'y', 'j'])  # doctest: +SKIP
    {'x', 'y'}
    """
    ret = []
    while tasks:
        work = []
        for w in tasks:
            typ = type(w)
            if typ is tuple and w and callable(w[0]):  # istask(w)
                work.extend(w[1:])
            elif typ is list:
                work.extend(w)
            elif typ is dict:
                work.extend(w.values())
            else:
                try:
                    if w in keys:
                        ret.append(w)
                except TypeError:  # not hashable
                    pass
        tasks = work
    return ret if as_list else set(ret)