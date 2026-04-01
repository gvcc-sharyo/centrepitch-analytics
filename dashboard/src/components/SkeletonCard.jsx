import styles from './SkeletonCard.module.css'

export function SkeletonCard({ height = 160 }) {
  return <div className={styles.skeleton} style={{ height }} />
}
