// Structure comes from a saved/validated definition. This layout is not a DAG
// validator, scheduler, event source, or evidence of execution.
const NODE_WIDTH = 240;
const PADDING = 24;
const COLUMN_GAP = 48;

function malformed() {
  throw new TypeError('Cannot render malformed dependency graph.');
}

function wrap(value, columns = 26) {
  const lines = [];
  let line = '', width = 0;
  for (const character of value) {
    const size = /\p{Mark}/u.test(character) ? 0 : character.codePointAt(0) > 255 ? 2 : 1;
    if (line && width + size > columns) {
      lines.push(line);
      line = '';
      width = 0;
    }
    line += character;
    width += size;
  }
  if (line) lines.push(line);
  return lines;
}

function structure(definition) {
  if (!Array.isArray(definition?.tasks) || !definition.tasks.length) malformed();
  const tasks = definition.tasks;
  const byKey = new Map(), parents = new Map(), children = new Map();
  for (const task of tasks) {
    if (!task || typeof task.task_id !== 'string' || !task.task_id || byKey.has(task.task_id)) malformed();
    const dependencies = task.depends_on === undefined ? [] : task.depends_on;
    if (!Array.isArray(dependencies) || new Set(dependencies).size !== dependencies.length) malformed();
    byKey.set(task.task_id, task);
    parents.set(task.task_id, dependencies);
    children.set(task.task_id, []);
  }
  for (const [key, dependencies] of parents) {
    for (const parent of dependencies) {
      if (!byKey.has(parent)) malformed();
      children.get(parent).push(key);
    }
  }
  const remaining = new Map([...parents].map(([key, dependencies]) => [key, dependencies.length]));
  const levels = new Map([...byKey.keys()].map(key => [key, 0]));
  const queue = [...byKey.keys()].filter(key => remaining.get(key) === 0);
  // Iteration avoids recursion overflow and fails closed on cyclic/missing data.
  for (let cursor = 0; cursor < queue.length; cursor++) {
    const key = queue[cursor];
    for (const child of children.get(key)) {
      levels.set(child, Math.max(levels.get(child), levels.get(key) + 1));
      remaining.set(child, remaining.get(child) - 1);
      if (remaining.get(child) === 0) queue.push(child);
    }
  }
  if (queue.length !== tasks.length) malformed();
  return {tasks, parents, children, levels};
}

export function layoutDefinitionDag(definition, {describeNode = () => ({})} = {}) {
  const {tasks, parents, children, levels} = structure(definition);
  const rows = [], nodes = [], byKey = new Map();
  for (const task of tasks) {
    const key = task.task_id, detail = describeNode(key) || {};
    const status = typeof detail.status === 'string' ? detail.status : '';
    const description = typeof detail.detail === 'string' ? detail.detail : '';
    const nameLines = wrap(key), statusLines = wrap(status), detailLines = wrap(description);
    const height = Math.max(64, 32 + nameLines.length * 20 +
      (statusLines.length ? 6 + statusLines.length * 18 : 0) +
      (detailLines.length ? 4 + detailLines.length * 18 : 0));
    const node = {key, level: levels.get(key), width: NODE_WIDTH, height,
      nameLines, statusLines, detailLines, status,
      title: [key, status, description, typeof detail.title === 'string' ? detail.title : ''].filter(Boolean).join('\n')};
    (rows[node.level] ||= []).push(node);
    nodes.push(node);
    byKey.set(key, node);
  }
  const edges = [];
  const boundaries = Array.from({length: rows.length - 1}, () => []);
  for (const task of tasks) {
    for (const parent of parents.get(task.task_id)) {
      const edge = {from: parent, to: task.task_id};
      edges.push(edge);
      const start = levels.get(parent), end = levels.get(task.task_id) - 1;
      boundaries[start].push(edge);
      if (end !== start) boundaries[end].push(edge);
    }
  }
  const contentWidth = Math.max(660, ...rows.map(row =>
    PADDING * 2 + row.length * NODE_WIDTH + (row.length - 1) * COLUMN_GAP));
  const rowBottoms = [];
  let y = PADDING;
  rows.forEach((row, level) => {
    const rowWidth = row.length * NODE_WIDTH + (row.length - 1) * COLUMN_GAP;
    row.forEach((node, index) => {
      node.x = (contentWidth - rowWidth) / 2 + index * (NODE_WIDTH + COLUMN_GAP);
      node.y = y;
    });
    rowBottoms.push(y + Math.max(...row.map(node => node.height)));
    y = rowBottoms[level] + (level < boundaries.length ? Math.max(64, 32 + boundaries[level].length * 12) : 0);
  });
  let skipLanes = 0;
  for (const edge of edges) {
    const source = byKey.get(edge.from), destination = byKey.get(edge.to);
    const outgoing = children.get(edge.from), incoming = parents.get(edge.to);
    const startX = source.x + NODE_WIDTH * (outgoing.indexOf(edge.to) + 1) / (outgoing.length + 1);
    const endX = destination.x + NODE_WIDTH * (incoming.indexOf(edge.from) + 1) / (incoming.length + 1);
    const startBoundary = source.level, endBoundary = destination.level - 1;
    const boundaryY = level => rowBottoms[level] + 16 + boundaries[level].indexOf(edge) * 12;
    const firstY = boundaryY(startBoundary), lastY = boundaryY(endBoundary);
    edge.points = [[startX, source.y + source.height], [startX, firstY]];
    if (startBoundary !== endBoundary) {
      // Cross-level edges use their own lane outside every node row; their
      // horizontal segments remain in row gaps, never through unrelated nodes.
      const lane = contentWidth + skipLanes++ * 18;
      edge.points.push([lane, firstY], [lane, lastY]);
    }
    edge.points.push([endX, lastY], [endX, destination.y]);
  }
  return {nodes, edges, width: contentWidth + (skipLanes ? skipLanes * 18 + PADDING : 0), height: y + PADDING};
}

export function drawDefinitionDag(target, definition, {describeNode, colorForStatus} = {}) {
  const layout = layoutDefinitionDag(definition, {describeNode});
  const document = target.ownerDocument || globalThis.document;
  const svg = (tag, attributes, text) => {
    const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
    for (const [name, value] of Object.entries(attributes)) node.setAttribute(name, value);
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const elements = [];
  for (const edge of layout.edges) {
    elements.push(svg('path', {
      d: edge.points.map(([x, y], index) => `${index ? 'L' : 'M'} ${x} ${y}`).join(' '),
      'data-from': edge.from, 'data-to': edge.to, role: 'img', 'aria-label': `${edge.from} → ${edge.to}`,
      fill: 'none', stroke: '#71869a', 'stroke-width': 1.7, 'stroke-linejoin': 'round'
    }));
    const [x, y] = edge.points.at(-1);
    elements.push(svg('path', {
      d: `M ${x - 4} ${y - 7} L ${x} ${y} L ${x + 4} ${y - 7}`,
      fill: 'none', stroke: '#71869a', 'stroke-width': 1.7, 'aria-hidden': 'true'
    }));
  }
  for (const node of layout.nodes) {
    const group = svg('g', {'data-task-key': node.key, role: 'img', 'aria-label': node.title});
    group.append(svg('title', {}, node.title));
    group.append(svg('rect', {x: node.x, y: node.y, width: node.width, height: node.height,
      rx: 8, fill: (node.status && colorForStatus?.(node.status)) || '#eef2f6',
      stroke: '#a8b7c5', 'stroke-width': 1}));
    let baseline = node.y + 28;
    for (const [kind, lines, gap, size] of [
      ['name', node.nameLines, 0, 20], ['status', node.statusLines, 6, 18], ['detail', node.detailLines, 4, 18]
    ]) {
      if (!lines.length) continue;
      baseline += gap;
      for (const line of lines) {
        group.append(svg('text', {x: node.x + 14, y: baseline, 'data-node-line': kind,
          'font-family': 'ui-monospace, SFMono-Regular, Consolas, monospace',
          'font-size': kind === 'name' ? 13 : 12, 'font-weight': kind === 'name' ? 600 : 400,
          fill: '#183247'}, line));
        baseline += size;
      }
    }
    elements.push(group);
  }
  target.setAttribute('viewBox', `0 0 ${layout.width} ${layout.height}`);
  target.setAttribute('width', layout.width);
  target.setAttribute('height', layout.height);
  target.style.width = layout.width + 'px';
  target.style.height = layout.height + 'px';
  target.replaceChildren(...elements);
  return layout;
}
