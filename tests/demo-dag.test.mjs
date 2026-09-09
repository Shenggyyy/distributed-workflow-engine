import assert from 'node:assert/strict';
import test from 'node:test';
import {drawDefinitionDag, layoutDefinitionDag} from '../src/workflow_engine/demo/static/dag.js';

const task = (task_id, ...depends_on) => ({task_id, depends_on});
const definition = (...tasks) => ({tasks});

class Node {
  constructor(tag, ownerDocument) {
    this.tag = tag;
    this.ownerDocument = ownerDocument;
    this.attributes = {};
    this.children = [];
    this.style = {};
    this.textContent = '';
  }
  setAttribute(key, value) { this.attributes[key] = String(value); }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
}

function target() {
  const document = {createElementNS(namespace, tag) {
    assert.equal(namespace, 'http://www.w3.org/2000/svg');
    return new Node(tag, document);
  }};
  return new Node('svg', document);
}

const nodes = svg => svg.children.filter(node => node.tag === 'g');
const lines = (node, kind) => node.children.filter(child => child.attributes['data-node-line'] === kind);

test('arbitrary saved topology and definition order determine layout, without mutation', () => {
  const source = definition(task('Report', 'Revenue', 'Quality', 'Read'),
    task('Quality', 'Read'), task('Read'), task('Revenue', 'Read'), task('Independent'));
  const before = JSON.stringify(source);
  const layout = layoutDefinitionDag(source);
  assert.deepEqual(layout.nodes.map(node => [node.key, node.level]), [
    ['Report', 2], ['Quality', 1], ['Read', 0], ['Revenue', 1], ['Independent', 0]
  ]);
  assert.deepEqual(layout.edges.map(edge => [edge.from, edge.to]), [
    ['Revenue', 'Report'], ['Quality', 'Report'], ['Read', 'Report'], ['Read', 'Quality'], ['Read', 'Revenue']
  ]);
  assert.equal(JSON.stringify(source), before);
  assert.ok(layout.nodes.find(node => node.key === 'Quality').x < layout.nodes.find(node => node.key === 'Revenue').x);
});

test('all edges point downward and skip-level paths avoid unrelated node interiors', () => {
  const source = definition(task('First'), task('Middle', 'First'), task('Side'),
    task('Deep', 'Middle'), task('Join', 'First', 'Middle', 'Deep', 'Side'));
  const {nodes, edges, width, height} = layoutDefinitionDag(source);
  for (const edge of edges) {
    const destination = nodes.find(node => node.key === edge.to);
    assert.equal(edge.points.at(-1)[1], destination.y);
    assert.ok(edge.points.at(-2)[1] < destination.y);
    for (const [x, y] of edge.points) assert.ok(x >= 0 && x <= width && y >= 0 && y <= height);
    for (let i = 1; i < edge.points.length; i++) {
      const [a, b] = [edge.points[i - 1], edge.points[i]];
      assert.ok(a[0] === b[0] || a[1] === b[1], 'Routes remain orthogonal');
      for (const node of nodes) {
        const crossing = a[0] === b[0] ?
          a[0] > node.x && a[0] < node.x + node.width &&
            Math.max(a[1], b[1]) > node.y && Math.min(a[1], b[1]) < node.y + node.height :
          a[1] > node.y && a[1] < node.y + node.height &&
            Math.max(a[0], b[0]) > node.x && Math.min(a[0], b[0]) < node.x + node.width;
        assert.equal(crossing, false, `${edge.from} → ${edge.to} must not cross ${node.key}`);
      }
    }
  }
});

test('full long names and Unicode are wrapped without truncation and expand node height', () => {
  const ascii = 'LongTask_' + 'x'.repeat(55);
  const unicode = '原始任务🙂'.repeat(9);
  const layout = layoutDefinitionDag(definition(task(ascii), task(unicode), task('Join', ascii, unicode)), {
    describeNode: key => ({status: 'RUNNING', detail: '#2 · 演示工作进程'.repeat(3), title: 'Raw ID: ' + key})
  });
  for (const node of layout.nodes) {
    assert.equal(node.nameLines.join(''), node.key);
    assert.ok(node.nameLines.every(line => Array.from(line).length <= 26));
    assert.ok(node.detailLines.length > 1);
    assert.ok(node.title.includes(node.key));
    assert.ok(node.title.includes('RUNNING'));
    assert.equal(node.detailLines.join(''), '#2 · 演示工作进程'.repeat(3));
  }
  assert.ok(layout.nodes[0].height > layout.nodes[2].height);
  assert.ok(layout.nodes[1].height > layout.nodes[2].height);
  assert.ok(layout.nodes[2].y > Math.max(...layout.nodes.slice(0, 2).map(node => node.y + node.height)));
});

test('a long serial definition is iterative and every real dependency is retained', () => {
  const tasks = Array.from({length: 1000}, (_, i) => task(`Task_${i}`, ...(i ? [`Task_${i - 1}`] : [])));
  const layout = layoutDefinitionDag(definition(...tasks));
  assert.equal(layout.nodes.length, 1000);
  assert.equal(layout.edges.length, 999);
  assert.equal(layout.nodes.at(-1).level, 999);
  assert.ok(layout.height > 1000);
  assert.equal(layout.width, 660);
});

test('malformed graph data fails closed without altering the previous drawing', () => {
  const svg = target();
  drawDefinitionDag(svg, definition(task('Original')));
  const original = svg.children;
  for (const source of [null, definition(), definition(task('Same'), task('Same')),
    definition(task('Missing', 'Absent')), definition({task_id: 'Invalid', depends_on: null}),
    definition(task('Loop', 'Loop')),
    definition(task('One', 'Two'), task('Two', 'One')), definition(task('Root'), task('End', 'Root', 'Root'))]) {
    assert.throws(() => drawDefinitionDag(svg, source), /malformed dependency graph/);
    assert.equal(svg.children, original);
  }
});

test('preview renders neutral name-only nodes, exact real edges and accessible literal identifiers', () => {
  const svg = target(), raw = '<script>raw&name</script>';
  const source = definition(task(raw), task('Final', raw));
  const layout = drawDefinitionDag(svg, source);
  assert.equal(nodes(svg).length, 2);
  const first = nodes(svg)[0];
  assert.equal(first.attributes['data-task-key'], raw);
  assert.equal(first.attributes['aria-label'], raw);
  assert.equal(first.children.find(node => node.tag === 'title').textContent, raw);
  assert.equal(lines(first, 'name').map(node => node.textContent).join(''), raw);
  assert.equal(lines(first, 'status').length, 0);
  assert.equal(lines(first, 'detail').length, 0);
  assert.ok(first.children.every(node => node.tag !== 'script'));
  assert.equal(svg.children.filter(node => node.attributes['data-from']).length, 1);
  assert.equal(svg.children.filter(node => node.attributes['aria-hidden'] === 'true').length, 1);
  assert.equal(svg.style.width, layout.width + 'px');
  assert.equal(svg.style.height, layout.height + 'px');
  assert.equal(svg.attributes.viewBox, `0 0 ${layout.width} ${layout.height}`);
});

test('running graph renders only supplied status and detail, preserving raw state codes', () => {
  const svg = target(), calls = [];
  drawDefinitionDag(svg, definition(task('Read'), task('Report', 'Read')), {
    describeNode: key => key === 'Read' ? {status: 'RETRY_WAIT', detail: '#2 · worker-original-id'} : {},
    colorForStatus: status => { calls.push(status); return '#123456'; }
  });
  const [read, report] = nodes(svg);
  assert.equal(lines(read, 'status')[0].textContent, 'RETRY_WAIT');
  assert.equal(lines(read, 'detail').map(node => node.textContent).join(''), '#2 · worker-original-id');
  assert.equal(read.children.find(node => node.tag === 'rect').attributes.fill, '#123456');
  assert.equal(lines(report, 'status').length, 0);
  assert.equal(lines(report, 'detail').length, 0);
  assert.deepEqual(calls, ['RETRY_WAIT']);
  drawDefinitionDag(svg, definition(task('Another')));
  assert.deepEqual(nodes(svg).map(node => node.attributes['data-task-key']), ['Another']);
});
