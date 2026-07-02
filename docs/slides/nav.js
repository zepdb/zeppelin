// Arrow-key navigation between chapters.
// Each page declares data-prev / data-next on <body>.
document.addEventListener('keydown', function (e) {
  if (e.metaKey || e.ctrlKey || e.altKey) return;
  var target = null;
  if (e.key === 'ArrowRight') target = document.body.dataset.next;
  if (e.key === 'ArrowLeft') target = document.body.dataset.prev;
  if (target) window.location.href = target;
});
