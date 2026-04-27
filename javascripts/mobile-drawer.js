/* Mobile drawer safety net:
   - Explicitly toggle the drawer from the hamburger button. Some
     mobile browsers get flaky when the button is only a <label>.
   - Close the drawer on any tap outside the centered card.
   - Close the drawer after tapping a navigation link. */
(function () {
  var MOBILE_QUERY = "(max-width: 76.1875em)";

  function isMobile() {
    return window.matchMedia(MOBILE_QUERY).matches;
  }

  function drawerToggle() {
    return document.getElementById("__drawer");
  }

  function drawerButton() {
    return document.querySelector('.md-header__button[for="__drawer"]');
  }

  function drawerCard() {
    return document.querySelector(".md-sidebar--primary .md-sidebar__scrollwrap");
  }

  function setDrawer(open) {
    var toggle = drawerToggle();
    if (!toggle) return;
    toggle.checked = open;
  }

  function syncDrawerClasses() {
    var toggle = drawerToggle();
    var open = !!(toggle && toggle.checked && isMobile());
    document.documentElement.classList.toggle("tl-drawer-open", open);
    document.body.classList.toggle("tl-drawer-open", open);
  }

  document.addEventListener("click", function (e) {
    var toggle = drawerToggle();
    var button = drawerButton();
    var card = drawerCard();

    if (!isMobile() || !toggle) return;

    if (button && button.contains(e.target)) {
      e.preventDefault();
      setDrawer(!toggle.checked);
      syncDrawerClasses();
      return;
    }

    if (!toggle.checked || !card) return;

    if (!card.contains(e.target)) {
      setDrawer(false);
      syncDrawerClasses();
      return;
    }

    if (e.target.closest(".md-sidebar--primary a")) {
      setDrawer(false);
      syncDrawerClasses();
    }
  }, true);

  document.addEventListener("change", function (e) {
    if (e.target && e.target.id === "__drawer") {
      syncDrawerClasses();
    }
  });

  window.addEventListener("resize", syncDrawerClasses);
  window.addEventListener("orientationchange", syncDrawerClasses);
  document.addEventListener("DOMContentLoaded", syncDrawerClasses);
  syncDrawerClasses();
})();
