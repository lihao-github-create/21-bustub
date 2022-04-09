//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) : rows_(rows), cols_(cols) { linear_ = new T[rows_ * cols_]; }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() { delete[] linear_; }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T *[rows];
    for (int i = 0; i < rows; i++) {
      data_[i] = Matrix<T>::linear_ + i * cols;
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return Matrix<T>::rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return Matrix<T>::cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if (i >= 0 && i < Matrix<T>::rows_ && j >= 0 && j < Matrix<T>::cols_) {
      return data_[i][j];
    }
    throw Exception(ExceptionType::OUT_OF_RANGE, "GetElement");
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i >= 0 && i < Matrix<T>::rows_ && j >= 0 && j < Matrix<T>::cols_) {
      data_[i][j] = val;
    } else {
      throw Exception(ExceptionType::OUT_OF_RANGE, "SetElement");
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    int src_size = source.size();
    if (src_size != Matrix<T>::rows_ * Matrix<T>::cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "FillFrom");
    }
    memcpy(Matrix<T>::linear_, source.data(), sizeof(T) * src_size);
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override { delete[] data_; }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear`
   * array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    int matrix_a_row = matrixA->GetRowCount();
    int matrix_a_col = matrixA->GetColumnCount();
    int matrix_b_row = matrixB->GetRowCount();
    int matrix_b_col = matrixB->GetColumnCount();
    if (matrix_a_row != matrix_b_row || matrix_a_col != matrix_b_col) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    auto ptr_result = std::make_unique<RowMatrix<T>>(matrix_b_row, matrix_b_col);
    for (int i = 0; i < matrix_b_row; i++) {
      for (int j = 0; j < matrix_b_col; j++) {
        T val = matrixA->GetElement(i, j) + matrixB->GetElement(i, j);
        ptr_result->SetElement(i, j, val);
      }
    }
    return ptr_result;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the
   * result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    int matrix_a_row = matrixA->GetRowCount();
    int matrix_a_col = matrixA->GetColumnCount();
    int matrix_b_row = matrixB->GetRowCount();
    int matrix_b_col = matrixB->GetColumnCount();
    if (matrix_a_col != matrix_b_row) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    auto ptr_result = std::make_unique<RowMatrix<T>>(matrix_a_row, matrix_b_col);
    for (int i = 0; i < matrix_a_row; i++) {
      for (int j = 0; j < matrix_b_col; j++) {
        T val = 0;
        for (int k = 0; k < matrix_a_col; k++) {
          val += matrixA->GetElement(i, k) * matrixB->GetElement(k, j);
        }
        ptr_result->SetElement(i, j, val);
      }
    }
    return ptr_result;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` *
   * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    std::unique_ptr<RowMatrix<T>> ptr_result;
    ptr_result = Multiply(matrixA, matrixB);
    if (ptr_result == nullptr) {
      return ptr_result;
    }
    ptr_result = Add(ptr_result.get(), matrixC);
    return ptr_result;
  }
};
}  // namespace bustub
