/* Mobile drawer safety net:
   - Close the drawer on any tap outside the centered card. The CSS
     already lets taps pass through the outer wrapper to Material's
     .md-overlay label, but real iOS Safari sometimes swallows the
     label tap. This handler guarantees the drawer closes. */
(function () {
  function isMobile() { return window.matchMedia("(max-width: 76.1875em)").matches; }

  document.addEventListener("click", function (e) {
    if (!isMobile()) return;
    var cb = document.getElementById("__drawer");
    if (!cb || !cb.checked) return;
    var card = document.querySelector(".md-sidebar--primary .md-sidebar__scrollwrap");
    if (!card) return;
    if (card.contains(e.target)) return;
    // Tap fell outside the card — close.
    cb.checked = false;
  }, true);
})();
