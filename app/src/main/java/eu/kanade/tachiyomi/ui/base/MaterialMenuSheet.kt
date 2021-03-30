package eu.kanade.tachiyomi.ui.base

import android.animation.ObjectAnimator
import android.animation.ValueAnimator
import android.app.Activity
import android.content.res.ColorStateList
import android.os.Build
import android.view.View
import android.view.ViewGroup
import android.view.ViewGroup.LayoutParams.MATCH_PARENT
import android.widget.ImageView
import android.widget.TextView
import androidx.annotation.DrawableRes
import androidx.annotation.StringRes
import com.google.android.material.bottomsheet.BottomSheetBehavior
import com.google.android.material.bottomsheet.BottomSheetDialog
import com.google.android.material.textview.MaterialTextView
import eu.kanade.tachiyomi.R
import eu.kanade.tachiyomi.databinding.BottomMenuSheetBinding
import eu.kanade.tachiyomi.util.system.dpToPx
import eu.kanade.tachiyomi.util.system.getResourceColor
import eu.kanade.tachiyomi.util.system.hasSideNavBar
import eu.kanade.tachiyomi.util.system.isInNightMode
import eu.kanade.tachiyomi.util.view.expand
import eu.kanade.tachiyomi.util.view.invisible
import eu.kanade.tachiyomi.util.view.isVisible
import eu.kanade.tachiyomi.util.view.setBottomEdge
import eu.kanade.tachiyomi.util.view.setEdgeToEdge
import eu.kanade.tachiyomi.util.view.setTextColorRes
import eu.kanade.tachiyomi.util.view.updateLayoutParams
import eu.kanade.tachiyomi.util.view.visible
import eu.kanade.tachiyomi.util.view.visibleIf

open class MaterialMenuSheet(
    activity: Activity,
    items: List<MenuSheetItem>,
    title: String? = null,
    selectedId: Int? = null,
    maxHeight: Int? = null,
    showDivider: Boolean = false,
    onMenuItemClicked: (MaterialMenuSheet, Int) -> Boolean
) :
    BottomSheetDialog
    (activity, R.style.BottomSheetDialogTheme) {

    private val primaryColor = activity.getResourceColor(android.R.attr.textColorPrimary)
    private val binding = BottomMenuSheetBinding.inflate(activity.layoutInflater)

    init {
        setContentView(binding.root)
        setEdgeToEdge(activity, binding.root)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && !context.isInNightMode() && !activity.window.decorView.rootWindowInsets.hasSideNavBar()) {
            window?.decorView?.systemUiVisibility = View.SYSTEM_UI_FLAG_LIGHT_NAVIGATION_BAR
        }
        maxHeight?.let {
            binding.menuScrollView.maxHeight = it + activity.window.decorView.rootWindowInsets.systemWindowInsetBottom
            binding.menuScrollView.requestLayout()
        }

        binding.divider.visibleIf(showDivider)
        var currentIndex: Int? = null
        items.forEachIndexed { index, item ->
            val view =
                activity.layoutInflater.inflate(R.layout.menu_sheet_item, null) as ViewGroup
            val textView = view.getChildAt(0) as MaterialTextView
            if (index == 0 && title == null) {
                view.setBackgroundResource(R.drawable.rounded_item_background)
            }
            with(view) {
                id = item.id
                binding.menuLayout.addView(this)
                setOnClickListener {
                    val shouldDismiss = onMenuItemClicked(this@MaterialMenuSheet, id)
                    if (shouldDismiss) {
                        dismiss()
                    }
                }
            }
            with(textView) {
                if (item.text != null) {
                    text = item.text
                } else {
                    setText(item.textRes)
                }
                setCompoundDrawablesRelativeWithIntrinsicBounds(item.drawable, 0, 0, 0)
                if (item.drawable == 0) {
                    textSize = 14f
                }
                if (item.id == selectedId) {
                    currentIndex = index
                    setTextColorRes(R.color.colorAccent)
                    compoundDrawableTintList =
                        ColorStateList.valueOf(context.getColor(R.color.colorAccent))
                }
                updateLayoutParams<ViewGroup.MarginLayoutParams> {
                    height = 48.dpToPx
                    width = MATCH_PARENT
                }
            }
        }

        BottomSheetBehavior.from(binding.root.parent as ViewGroup).expand()
        BottomSheetBehavior.from(binding.root.parent as ViewGroup).skipCollapsed = true

        setBottomEdge(binding.menuLayout, activity)

        binding.titleLayout.visibleIf(title != null)
        binding.toolbarTitle.text = title

        currentIndex?.let {
            binding.root.post {
                binding.menuScrollView?.scrollTo(0, it * 48.dpToPx - binding.menuScrollView.height / 2)
            }
        }

        var isElevated = false
        var elevationAnimator: ValueAnimator? = null

        fun elevate(elevate: Boolean) {
            elevationAnimator?.cancel()
            isElevated = elevate
            elevationAnimator?.cancel()
            elevationAnimator = ObjectAnimator.ofFloat(
                binding.titleLayout,
                "elevation",
                binding.titleLayout.elevation,
                if (elevate) 10f else 0f
            )
            elevationAnimator?.start()
        }
        elevate(binding.menuScrollView.canScrollVertically(-1))
        if (binding.titleLayout.isVisible()) {
            binding.menuScrollView.setOnScrollChangeListener { _: View?, _: Int, _: Int, _: Int, _: Int ->
                val notAtTop = binding.menuScrollView.canScrollVertically(-1)
                if (notAtTop != isElevated) {
                    elevate(notAtTop)
                }
            }
        }
    }

    private fun clearEndDrawables() {
        (0 until binding.menuLayout.childCount).forEach {
            val textView = (binding.menuLayout.getChildAt(it) as ViewGroup).getChildAt(0) as TextView
            val imageView = (binding.menuLayout.getChildAt(it) as ViewGroup).getChildAt(1) as ImageView
            textView.setTextColor(primaryColor)
            textView.compoundDrawableTintList = ColorStateList.valueOf(primaryColor)
            imageView.invisible()
        }
    }

    fun setDrawable(id: Int, @DrawableRes drawableRes: Int, clearAll: Boolean = true) {
        if (clearAll) {
            clearEndDrawables()
        }
        val layout = binding.menuLayout.findViewById<ViewGroup>(id) ?: return
        val textView = layout.getChildAt(0) as? TextView
        val imageView = layout.getChildAt(1) as? ImageView
        textView?.setTextColorRes(R.color.colorAccent)
        textView?.compoundDrawableTintList =
            ColorStateList.valueOf(context.getColor(R.color.colorAccent))
        imageView?.visible()
        imageView?.setImageResource(drawableRes)
    }

    data class MenuSheetItem(
        val id: Int,
        @DrawableRes val drawable: Int = 0,
        @StringRes val textRes: Int = 0,
        val text: String? = null
    )
}
